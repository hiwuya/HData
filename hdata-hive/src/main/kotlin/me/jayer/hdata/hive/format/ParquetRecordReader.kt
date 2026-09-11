package me.jayer.hdata.hive.format

import me.jayer.hdata.core.type.FieldTypes
import me.jayer.hdata.hive.split.HiveFile
import me.jayer.hdata.hive.SampleMethod
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.column.statistics.Statistics
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.beam.sdk.metrics.Metrics
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset

/**
 * Parquet reader, using parquet-hadoop directly, without Hive's `MapredParquetInputFormat`.
 *
 * The claimable boundary is the **row group**: `ParquetFileReader` filters row groups by byte range once when opening (whether a
 * row group falls inside the range is decided by its midpoint, consistent with parquet's own `withRange`), and afterwards they
 * are claimed and read one by one.
 *
 * Records are materialized by parquet's own `GroupRecordConverter`, i.e. they become a `Group` first and then a Beam `Row`. That
 * costs one intermediate object compared with writing our own `RecordMaterializer`, but saves several hundred lines of column
 * converter code; the bottleneck of a sync job is IO, so this overhead is acceptable.
 *
 * @author wuya
 */
class ParquetRecordReader(
    private val file: HiveFile,
    private val range: OffsetRange,
    spec: HiveReadSpec,
    partitionValues: List<Any?>,
    private val configuration: Configuration,
) : HiveRecordReader(spec, partitionValues) {

    private var fileReader: ParquetFileReader? = null

    override fun read(claim: OffsetClaim, output: (Row) -> Unit): Boolean {
        val path = Path(file.path)
        val options = HadoopReadOptions.builder(configuration, path).build()
        val reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, configuration), options)
        fileReader = reader

        val fileSchema = reader.footer.fileMetaData.schema
        val requestedSchema = project(fileSchema)
        val columnIO = ColumnIOFactory().getColumnIO(requestedSchema, fileSchema, true)
        val converter = GroupRecordConverter(requestedSchema)
        val fieldTypes = spec.dataFieldTypes
        val requestedFields = requestedSchema.fields

        // A row group belongs to this range if and only if its **start offset** falls inside [from, to) (the same criterion as
        // ORC's stripe start offset ownership: every row group's start point is unique, so neighbouring ranges are neither
        // overlapping nor leaking).
        //
        // Note: parquet's HadoopReadOptions.withRange only affects byte prefetching, it does not filter row groups —
        // getRowGroups() returns **all** row groups in the file and readNextRowGroup() also reads them all in order. So we must
        // not walk all row groups while calling readNextRowGroup(): every split would read the whole file once and the data would
        // be duplicated N times with N splits. After filtering by start offset, readRowGroup(index) reads exactly those row
        // groups inside this range, by absolute index.
        var claimed = -1L
        val predicates = spec.predicates
        // Block sampling (SYSTEM): each row group is skipped as a whole with probability fraction, saving the IO outright (Trino's TABLESAMPLE SYSTEM equivalent).
        val doSystemSample = spec.sampleMethod == SampleMethod.SYSTEM && spec.sampleFraction < 1.0
        val systemSeed = if (doSystemSample) checkNotNull(spec.sampleSeed) else 0L
        for ((index, block) in reader.rowGroups.withIndex()) {
            // A row group belongs to this range if and only if its start offset falls inside [from, to) (the same criterion as ORC's
            // stripe start offset ownership). Row groups are ordered by increasing start offset, so once the end of the range is
            // passed there is no need to walk the remaining row groups in the file.
            if (block.startingPos >= range.to) {
                break
            }
            if (block.startingPos < range.from) {
                continue
            }
            // Row groups skipped by sampling/predicates also count as completed work and must be claimed first, so that the runtime
            // residual does not pick up the same block again; block offsets increase strictly, so OffsetRangeTracker's contract holds.
            if (block.startingPos > claimed) {
                if (!claim.tryClaim(block.startingPos)) {
                    return false
                }
                claimed = block.startingPos
            }
            // Block sampling: when this row group is drawn to be "dropped", skip it without reading.
            if (doSystemSample && sampleBlock(systemSeed, file.path, block.startingPos) >= spec.sampleFraction) {
                Metrics.counter(ParquetRecordReader::class.java, "parquetRowGroupsSkipped").inc()
                continue
            }
            // Predicate pushdown: use the row group's column statistics (min/max/null) to decide the whole block cannot match and
            // skip it. Without predicates the original fast path is used and statistics are never touched.
            if (predicates.isNotEmpty()) {
                val stats = collectParquetStats(block, fileSchema, predicates)
                if (PredicateEvaluator.canSkip(predicates, stats)) {
                    Metrics.counter(ParquetRecordReader::class.java, "parquetRowGroupsSkipped").inc()
                    continue
                }
            }
            val pages = reader.readRowGroup(index) ?: return true
            val recordReader = columnIO.getRecordReader(pages, converter)
            repeat(pages.rowCount.toInt()) {
                val group = recordReader.read()
                val values = Array<Any?>(spec.projectedDataIndexes.size) { i ->
                    val fieldName = spec.projectedDataColumns[i].name
                    val fieldIndex = requestedFields.indexOfFirst { it.name.equals(fieldName, ignoreCase = true) }
                    if (fieldIndex < 0) {
                        null
                    } else {
                        readField(group, fieldIndex, requestedFields[fieldIndex], fieldTypes[spec.projectedDataIndexes[i]])
                    }
                }
                output(toRow(values))
            }
        }
        return true
    }

    /**
     * Collects the Parquet column statistics (min/max/null) of each predicate column in this row group, used to decide whether
     * the whole block can be skipped. Columns whose type is not numeric/string, and row groups without statistics, return null.
     */
    private fun collectParquetStats(
        block: BlockMetaData,
        fileSchema: MessageType,
        predicates: List<HivePredicate>,
    ): Map<String, ColumnRangeStats> {
        val columns = block.columns
        val result = mutableMapOf<String, ColumnRangeStats>()
        for (p in predicates) {
            if (p.column in result) {
                continue
            }
            val ref = parquetTopLevelColumn(fileSchema, p.column)
            if (ref == null || ref.leafOrdinal >= columns.size) {
                continue
            }
            val stats = columns[ref.leafOrdinal].statistics
            // decimal is stored in parquet as an "unscaled value" and the statistics are unscaled too; converting back to BigDecimal needs the column's scale
            val scale = if (p.fieldType.typeName == Schema.TypeName.DECIMAL) {
                (ref.type.logicalTypeAnnotation as? LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)?.scale ?: 0
            } else {
                0
            }
            val (min, max) = extractParquetRange(stats, p.fieldType, scale)
            // parquet 1.17 has no hasNull(), so derive it from numNulls: numNulls == 0 -> no NULL in this row group; numNulls ==
            // rowCount -> the whole column is NULL (then `col IS NOT NULL` can skip the whole block).
            val hasNull = !(stats.isNumNullsSet && stats.numNulls == 0L)
            val allNull = stats.isNumNullsSet && stats.numNulls == block.rowCount
            result[p.column] = ColumnRangeStats(min, max, hasNull, allNull)
        }
        return result
    }

    private fun extractParquetRange(
        stats: Statistics<*>,
        fieldType: Schema.FieldType,
        decimalScale: Int = 0,
    ): Pair<ValueRepr?, ValueRepr?> = parquetColumnRange(stats, fieldType, decimalScale)

    /**
     * Read only the projected columns.
     *
     * Matching by **column name** (case-insensitive): the column names in a Parquet file were written by Hive and match the table
     * definition; columns missing from the file (added to the table later) are left out of requestedSchema and read back as null.
     */
    private fun project(fileSchema: MessageType): MessageType {
        val fields = spec.projectedDataColumns.mapNotNull { column ->
            fileSchema.fields.firstOrNull { it.name.equals(column.name, ignoreCase = true) }
        }
        if (fields.size < spec.projectedDataColumns.size) {
            LOGGER.info(
                "Parquet file[{}] is missing these columns, they will be read as null: {}",
                file.path,
                spec.projectedDataColumns.map { it.name }
                    .filterNot { name -> fields.any { it.name.equals(name, ignoreCase = true) } },
            )
        }
        return MessageType(fileSchema.name, fields)
    }

    private fun readField(group: Group, index: Int, type: Type, fieldType: Schema.FieldType): Any? {
        if (group.getFieldRepetitionCount(index) == 0) {
            return null
        }
        return toValue(group, index, 0, type, fieldType)
    }

    private fun toValue(
        group: Group,
        fieldIndex: Int,
        valueIndex: Int,
        type: Type,
        fieldType: Schema.FieldType,
    ): Any? {
        val target = fieldType.withNullable(false)
        if (!type.isPrimitive) {
            val nested = group.getGroup(fieldIndex, valueIndex)
            return when (type.logicalTypeAnnotation) {
                is LogicalTypeAnnotation.ListLogicalTypeAnnotation -> readList(nested, type.asGroupType(), target)
                is LogicalTypeAnnotation.MapLogicalTypeAnnotation -> readMap(nested, type.asGroupType(), target)
                else -> readStruct(nested, type.asGroupType(), target)
            }
        }
        return readPrimitive(group, fieldIndex, valueIndex, type.asPrimitiveType(), target)
    }

    /**
     * Both the standard three-level LIST (`list` -> `element`) and the two-level form written by early Hive (`bag` ->
     * `array_element`) must be recognized: the middle level is always `repeated` and the element is the only field below it; if
     * the repeated level is the element itself (only a group with a single field counts as a wrapper), treat it as the element.
     */
    private fun readList(listGroup: Group, listType: GroupType, target: Schema.FieldType): List<Any?> {
        if (listType.fieldCount == 0) {
            return emptyList()
        }
        val repeatedType = listType.getType(0)
        val elementFieldType = target.collectionElementType!!
        val count = listGroup.getFieldRepetitionCount(0)
        if (repeatedType.isPrimitive) {
            // repeated is the element directly (one of the two-level forms)
            return (0 until count).map { i -> readPrimitive(listGroup, 0, i, repeatedType.asPrimitiveType(), elementFieldType.withNullable(false)) }
        }
        val repeatedGroup = repeatedType.asGroupType()
        if (repeatedGroup.fieldCount != 1) {
            // repeated is the element itself (the two-level form of a struct list)
            return (0 until count).map { i ->
                readStruct(listGroup.getGroup(0, i), repeatedGroup, elementFieldType.withNullable(false))
            }
        }
        val elementType = repeatedGroup.getType(0)
        return (0 until count).map { i ->
            val entry = listGroup.getGroup(0, i)
            if (entry.getFieldRepetitionCount(0) == 0) {
                null
            } else {
                toValue(entry, 0, 0, elementType, elementFieldType)
            }
        }
    }

    private fun readMap(mapGroup: Group, mapType: GroupType, target: Schema.FieldType): Map<Any?, Any?> {
        if (mapType.fieldCount == 0) {
            return emptyMap()
        }
        val keyValueType = mapType.getType(0).asGroupType()
        val count = mapGroup.getFieldRepetitionCount(0)
        return (0 until count).mapNotNull { i ->
            val entry = mapGroup.getGroup(0, i)
            val key = toValue(entry, 0, 0, keyValueType.getType(0), target.mapKeyType!!) ?: return@mapNotNull null
            val value = if (keyValueType.fieldCount < 2 || entry.getFieldRepetitionCount(1) == 0) {
                null
            } else {
                toValue(entry, 1, 0, keyValueType.getType(1), target.mapValueType!!)
            }
            key to value
        }.toMap()
    }

    private fun readStruct(group: Group, groupType: GroupType, target: Schema.FieldType): Row {
        val rowSchema = target.rowSchema!!
        val builder = Row.withSchema(rowSchema)
        rowSchema.fields.forEach { field ->
            val index = groupType.fields.indexOfFirst { it.name.equals(field.name, ignoreCase = true) }
            builder.addValue(
                if (index < 0 || group.getFieldRepetitionCount(index) == 0) {
                    null
                } else {
                    toValue(group, index, 0, groupType.getType(index), field.type)
                }
            )
        }
        return builder.build()
    }

    private fun readPrimitive(
        group: Group,
        fieldIndex: Int,
        valueIndex: Int,
        type: PrimitiveType,
        target: Schema.FieldType,
    ): Any? {
        val annotation = type.logicalTypeAnnotation
        return when (type.primitiveTypeName) {
            PrimitiveType.PrimitiveTypeName.BOOLEAN -> group.getBoolean(fieldIndex, valueIndex)

            PrimitiveType.PrimitiveTypeName.INT32 -> {
                val value = group.getInteger(fieldIndex, valueIndex)
                when (annotation) {
                    is LogicalTypeAnnotation.DateLogicalTypeAnnotation -> LocalDate.ofEpochDay(value.toLong())
                    is LogicalTypeAnnotation.DecimalLogicalTypeAnnotation ->
                        BigDecimal.valueOf(value.toLong(), annotation.scale)

                    else -> narrow(value.toLong(), target)
                }
            }

            PrimitiveType.PrimitiveTypeName.INT64 -> {
                val value = group.getLong(fieldIndex, valueIndex)
                when (annotation) {
                    is LogicalTypeAnnotation.DecimalLogicalTypeAnnotation -> BigDecimal.valueOf(value, annotation.scale)
                    is LogicalTypeAnnotation.TimestampLogicalTypeAnnotation -> timestamp(value, annotation, target)
                    else -> narrow(value, target)
                }
            }

            PrimitiveType.PrimitiveTypeName.FLOAT -> group.getFloat(fieldIndex, valueIndex)
            PrimitiveType.PrimitiveTypeName.DOUBLE -> group.getDouble(fieldIndex, valueIndex)

            // Early Hive / Impala used int96 for timestamp: the first 8 bytes are nanoseconds within the day, the last 4 the Julian day
            PrimitiveType.PrimitiveTypeName.INT96 ->
                int96ToInstant(group.getInt96(fieldIndex, valueIndex)).let {
                    if (target == FieldTypes.TIMESTAMP) it else LocalDateTime.ofInstant(it, ZoneOffset.UTC)
                }

            PrimitiveType.PrimitiveTypeName.BINARY,
            PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
            -> {
                val binary = group.getBinary(fieldIndex, valueIndex)
                when {
                    annotation is LogicalTypeAnnotation.DecimalLogicalTypeAnnotation ->
                        BigDecimal(BigInteger(binary.bytes), annotation.scale)

                    target.typeName == Schema.TypeName.BYTES -> binary.bytes
                    else -> binary.toStringUsingUTF8()
                }
            }

            else -> throw UnsupportedOperationException("unsupported Parquet type: ${type.primitiveTypeName}")
        }
    }

    /** parquet only has int32 / int64 integers, so columns declared tinyint / smallint in the table must be narrowed back. */
    private fun narrow(value: Long, target: Schema.FieldType): Any = when (target.typeName) {
        Schema.TypeName.BYTE -> value.toByte()
        Schema.TypeName.INT16 -> value.toShort()
        Schema.TypeName.INT32 -> value.toInt()
        Schema.TypeName.INT64 -> value
        Schema.TypeName.DOUBLE -> value.toDouble()
        Schema.TypeName.FLOAT -> value.toFloat()
        Schema.TypeName.DECIMAL -> BigDecimal.valueOf(value)
        else -> value
    }

    private fun timestamp(
        value: Long,
        annotation: LogicalTypeAnnotation.TimestampLogicalTypeAnnotation,
        target: Schema.FieldType,
    ): Any {
        val instant = when (annotation.unit) {
            LogicalTypeAnnotation.TimeUnit.MILLIS -> Instant.ofEpochMilli(value)
            LogicalTypeAnnotation.TimeUnit.MICROS ->
                Instant.ofEpochSecond(Math.floorDiv(value, 1_000_000L), Math.floorMod(value, 1_000_000L) * 1_000L)

            LogicalTypeAnnotation.TimeUnit.NANOS ->
                Instant.ofEpochSecond(Math.floorDiv(value, 1_000_000_000L), Math.floorMod(value, 1_000_000_000L))
        }
        return if (target == FieldTypes.TIMESTAMP) {
            instant
        } else {
            LocalDateTime.ofInstant(instant, ZoneOffset.UTC)
        }
    }

    private fun int96ToInstant(binary: Binary): Instant {
        val buffer = binary.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN)
        val nanosOfDay = buffer.long
        val julianDay = buffer.int
        val epochDay = julianDay - JULIAN_EPOCH_OFFSET_DAYS
        return Instant.ofEpochSecond(epochDay * 86_400L, nanosOfDay)
    }

    override fun close() {
        fileReader?.close()
        fileReader = null
    }

    private companion object {
        val LOGGER = LoggerFactory.getLogger(ParquetRecordReader::class.java)

        /** Julian day 2440588 is 1970-01-01. */
        const val JULIAN_EPOCH_OFFSET_DAYS = 2_440_588L
    }
}
