package me.jayer.hdata.hive.format

import me.jayer.hdata.hive.split.HiveFile
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector
import org.apache.orc.ColumnStatistics
import org.apache.orc.DecimalColumnStatistics
import org.apache.orc.DoubleColumnStatistics
import org.apache.orc.IntegerColumnStatistics
import org.apache.orc.OrcFile
import org.apache.orc.Reader
import org.apache.orc.StripeInformation
import org.apache.orc.StripeStatistics
import org.apache.orc.StringColumnStatistics
import org.apache.orc.TypeDescription
import org.apache.beam.sdk.metrics.Metrics
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import me.jayer.hdata.hive.SampleMethod
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset

/**
 * ORC reader, using orc-core's vectorized interface directly, without Hive's `OrcSerde` / `OrcInputFormat`.
 *
 * The claimable boundary is the **stripe**: the ORC file tail records each stripe's offset and length, and the stripes falling
 * inside this range are claimed and read one by one. Only then can the runtime hand the remaining stripes to idle workers
 * halfway through — claiming the whole range at once makes that impossible.
 *
 * Timestamps are always read as UTC (`useUTCTimestamp(true)`). ORC's TIMESTAMP by default converts "writer time zone -> reader
 * time zone", so the same file read on machines in different time zones yields different wall-clock times; a sync tool has to
 * eliminate that uncertainty, and the write side enables the same switch so both ends agree.
 *
 * @author wuya
 */
class OrcRecordReader(
    private val file: HiveFile,
    private val range: OffsetRange,
    spec: HiveReadSpec,
    partitionValues: List<Any?>,
    private val configuration: Configuration,
) : HiveRecordReader(spec, partitionValues) {

    private var reader: Reader? = null

    override fun read(claim: OffsetClaim, output: (Row) -> Unit): Boolean {
        val path = Path(file.path)
        val orcReader = OrcFile.createReader(
            path,
            OrcFile.readerOptions(configuration).filesystem(path.getFileSystem(configuration)).useUTCTimestamp(true),
        )
        reader = orcReader

        val fileSchema = orcReader.schema
        val fieldTypes = spec.dataFieldTypes
        val mapping = resolveColumns(fileSchema)
        val include = includeMask(fileSchema, mapping)

        val predicates = spec.predicates
        // Block sampling (SYSTEM): each stripe is skipped as a whole with probability fraction, saving the IO outright (Trino's TABLESAMPLE SYSTEM equivalent).
        val doSystemSample = spec.sampleMethod == SampleMethod.SYSTEM && spec.sampleFraction < 1.0
        val systemSeed = if (doSystemSample) checkNotNull(spec.sampleSeed) else 0L
        // Read all stripe statistics in one go only when there are predicates; without predicates statistics are never touched,
        val stripeStats = if (predicates.isEmpty()) emptyList() else orcReader.stripeStatistics
        for ((i, stripe) in orcReader.stripes.withIndex()) {
            // Stripe ownership is decided by start offset, the same criterion Trino / Hive use when splitting: stripes falling
            // inside this range are claimed and read one by one. Stripes are ordered by increasing offset, so once the end of the
            // range is passed there is no need to walk the remaining stripes in the file.
            if (stripe.offset >= range.to) {
                break
            }
            if (stripe.offset < range.from) {
                continue
            }
            // Even a stripe that will be skipped wholesale by sampling/predicates must be claimed first: skipping is also
            // completing that work. Otherwise runtime splitting may hand this not-yet-claimed stripe to the residual restriction as well.
            if (!claim.tryClaim(stripe.offset)) {
                return false
            }
            // Block sampling: when this stripe is drawn to be "dropped", skip it without reading.
            if (doSystemSample && sampleBlock(systemSeed, file.path, stripe.offset) >= spec.sampleFraction) {
                Metrics.counter(OrcRecordReader::class.java, "orcStripesSkipped").inc()
                continue
            }
            // Predicate pushdown: use the stripe's column statistics (min/max/null) to decide the whole block cannot match and skip.
            if (predicates.isNotEmpty()) {
                val stats = collectOrcStats(fileSchema, stripeStats[i], predicates)
                if (PredicateEvaluator.canSkip(predicates, stats)) {
                    Metrics.counter(OrcRecordReader::class.java, "orcStripesSkipped").inc()
                    continue
                }
            }
            readStripe(orcReader, stripe.offset, stripe.length, include, fileSchema, mapping, fieldTypes, output)
        }
        return true
    }

    /**
     * Collects the ORC column statistics (min/max/null) of each predicate column in this stripe, used to decide whether the whole
     * block can be skipped. Columns whose type is not numeric/string, and stripes without statistics, return null (never skip).
     */
    private fun collectOrcStats(
        fileSchema: TypeDescription,
        stripeStatistics: StripeStatistics,
        predicates: List<HivePredicate>,
    ): Map<String, ColumnRangeStats> {
        val columns = stripeStatistics.columnStatistics
        val result = mutableMapOf<String, ColumnRangeStats>()
        for (p in predicates) {
            if (p.column in result) {
                continue
            }
            val dataIndex = spec.dataColumns.indexOfFirst { it.name.equals(p.column, ignoreCase = true) }
            val ref = orcTopLevelColumn(fileSchema, p.column, dataIndex) ?: continue
            if (ref.id >= columns.size) {
                continue
            }
            val colStats = columns[ref.id]
            val scale = if (p.fieldType.typeName == Schema.TypeName.DECIMAL) {
                if (ref.type.category == TypeDescription.Category.DECIMAL) ref.type.scale else 0
            } else {
                0
            }
            val (min, max) = extractOrcRange(colStats, p.fieldType, scale)
            // ORC's ColumnStatistics only exposes hasNull (whether NULLs exist) with no null count, so "the whole column is NULL"
            // cannot be proven; whole-block skipping for IS NOT NULL is therefore conservatively not triggered on ORC (allNull=false); only the row-level filter covers it.
            result[p.column] = ColumnRangeStats(min, max, colStats.hasNull(), false)
        }
        return result
    }

    private fun extractOrcRange(
        colStats: ColumnStatistics,
        fieldType: Schema.FieldType,
        decimalScale: Int,
    ): Pair<ValueRepr?, ValueRepr?> = orcColumnRange(colStats, fieldType, decimalScale)

    private fun readStripe(
        orcReader: Reader,
        offset: Long,
        length: Long,
        include: BooleanArray,
        fileSchema: TypeDescription,
        mapping: IntArray,
        fieldTypes: List<Schema.FieldType>,
        output: (Row) -> Unit,
    ) {
        val options = orcReader.options().range(offset, length).include(include)
        orcReader.rows(options).use { rows ->
            val batch = fileSchema.createRowBatch()
            val children = fileSchema.children
            while (rows.nextBatch(batch)) {
                for (rowIndex in 0 until batch.size) {
                    val values = Array<Any?>(mapping.size) { i ->
                        val fileIndex = mapping[i]
                        if (fileIndex < 0) {
                            null
                        } else {
                            toValue(
                                batch.cols[fileIndex],
                                rowIndex,
                                children[fileIndex],
                                fieldTypes[spec.projectedDataIndexes[i]],
                            )
                        }
                    }
                    output(toRow(values))
                }
            }
        }
    }

    /**
     * Index of each projected column inside the ORC file, -1 when the file lacks that column (an old file written before the
     *
     * Matching by **column name** is preferred; when the names in the file do not line up (ORC written by early Hive versions
     * has fields literally named `_col0`, `_col1`) it falls back to matching by **position**. Trino exposes this choice to the
     * user through the `hive.orc.use-column-names` switch; here it is decided automatically: names are used only when every
     */
    private fun resolveColumns(fileSchema: TypeDescription): IntArray {
        require(fileSchema.category == TypeDescription.Category.STRUCT) {
            "the top-level type of an ORC file should be struct, but is ${fileSchema.category}: ${file.path}"
        }
        val fileNames = fileSchema.fieldNames
        val byName = spec.projectedDataColumns.map { column ->
            fileNames.indexOfFirst { it.equals(column.name, ignoreCase = true) }
        }
        if (byName.none { it < 0 }) {
            return byName.toIntArray()
        }
        LOGGER.info(
            "ORC file[{}] column names do not match the table definition (the file has {}), falling back to matching by position",
            file.path,
            fileNames.take(8),
        )
        return spec.projectedDataIndexes.map { if (it < fileSchema.children.size) it else -1 }.toIntArray()
    }

    /** Read only the projected columns. ORC's include array is indexed by [TypeDescription.getId], so the whole subtree is marked. */
    private fun includeMask(fileSchema: TypeDescription, mapping: IntArray): BooleanArray {
        val include = BooleanArray(fileSchema.maximumId + 1)
        include[0] = true
        mapping.filter { it >= 0 }.forEach { index ->
            val child = fileSchema.children[index]
            for (id in child.id..child.maximumId) {
                include[id] = true
            }
        }
        return include
    }

    private fun toValue(
        vector: ColumnVector,
        row: Int,
        type: TypeDescription,
        fieldType: Schema.FieldType,
    ): Any? {
        // An isRepeating vector only holds a value at position 0
        val index = if (vector.isRepeating) 0 else row
        if (!vector.noNulls && vector.isNull[index]) {
            return null
        }
        return when (type.category) {
            TypeDescription.Category.BOOLEAN -> (vector as LongColumnVector).vector[index] != 0L
            TypeDescription.Category.BYTE -> (vector as LongColumnVector).vector[index].toByte()
            TypeDescription.Category.SHORT -> (vector as LongColumnVector).vector[index].toShort()
            TypeDescription.Category.INT -> (vector as LongColumnVector).vector[index].toInt()
            TypeDescription.Category.LONG -> (vector as LongColumnVector).vector[index]
            TypeDescription.Category.FLOAT -> (vector as DoubleColumnVector).vector[index].toFloat()
            TypeDescription.Category.DOUBLE -> (vector as DoubleColumnVector).vector[index]
            TypeDescription.Category.DATE -> LocalDate.ofEpochDay((vector as LongColumnVector).vector[index])

            TypeDescription.Category.STRING,
            TypeDescription.Category.VARCHAR,
            TypeDescription.Category.CHAR,
            -> (vector as BytesColumnVector).toString(index)

            TypeDescription.Category.BINARY -> (vector as BytesColumnVector).let {
                it.vector[index].copyOfRange(it.start[index], it.start[index] + it.length[index])
            }

            // HiveDecimal strips trailing zeros: 4.50 in a decimal(10,2) column comes out as 4.5 with scale 1, numerically equal
            // but not equals. Pad it back to the scale declared on the column
            TypeDescription.Category.DECIMAL ->
                (vector as DecimalColumnVector).vector[index].hiveDecimal.bigDecimalValue()
                    .setScale(type.scale, java.math.RoundingMode.HALF_UP)

            TypeDescription.Category.TIMESTAMP -> (vector as TimestampColumnVector).let {
                LocalDateTime.ofInstant(instantAt(it, index), ZoneOffset.UTC)
            }

            TypeDescription.Category.TIMESTAMP_INSTANT -> instantAt(vector as TimestampColumnVector, index)

            TypeDescription.Category.LIST -> {
                val list = vector as ListColumnVector
                val elementType = fieldType.withNullable(false).collectionElementType!!
                val offset = list.offsets[index].toInt()
                (0 until list.lengths[index].toInt()).map { i ->
                    toValue(list.child, offset + i, type.children[0], elementType)
                }
            }

            TypeDescription.Category.MAP -> {
                val map = vector as MapColumnVector
                val target = fieldType.withNullable(false)
                val offset = map.offsets[index].toInt()
                (0 until map.lengths[index].toInt()).mapNotNull { i ->
                    val key = toValue(map.keys, offset + i, type.children[0], target.mapKeyType!!)
                        ?: return@mapNotNull null
                    key to toValue(map.values, offset + i, type.children[1], target.mapValueType!!)
                }.toMap()
            }

            TypeDescription.Category.STRUCT -> {
                val struct = vector as StructColumnVector
                val rowSchema = fieldType.withNullable(false).rowSchema!!
                val builder = Row.withSchema(rowSchema)
                rowSchema.fields.forEachIndexed { i, field ->
                    builder.addValue(
                        if (i < struct.fields.size) {
                            toValue(struct.fields[i], index, type.children[i], field.type)
                        } else {
                            null
                        }
                    )
                }
                builder.build()
            }

            else -> throw UnsupportedOperationException("unsupported ORC type: ${type.category} (column ${type.id})")
        }
    }

    private fun instantAt(vector: TimestampColumnVector, index: Int): Instant =
        Instant.ofEpochMilli(vector.time[index]).plusNanos((vector.nanos[index] % 1_000_000).toLong())

    override fun close() {
        reader?.close()
        reader = null
    }

    private companion object {
        val LOGGER = LoggerFactory.getLogger(OrcRecordReader::class.java)
    }
}
