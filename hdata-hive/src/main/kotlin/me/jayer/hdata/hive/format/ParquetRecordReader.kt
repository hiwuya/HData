package me.jayer.hdata.hive.format

import me.jayer.hdata.hive.split.HiveFile
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
 * Parquet 读取器，直接用 parquet-hadoop 读，不经过 Hive 的 `MapredParquetInputFormat`。
 *
 * 可认领的边界是 **row group**：`ParquetFileReader` 打开时就按字节区间筛过一遍 row group
 * （落在区间内的判定用的是 row group 的中点，和 parquet 自己的 `withRange` 一致），
 * 之后逐个认领、逐个读。
 *
 * 记录的物化走 parquet 自带的 `GroupRecordConverter`，也就是先变成 `Group` 再转 Beam `Row`。
 * 比起自己写一套 `RecordMaterializer` 多了一次中间对象，但省下几百行列转换器代码；
 * 同步作业的瓶颈在 IO，这一层开销可以接受。
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

        // 一个 row group 属于本区间，当且仅当它的**起始偏移量**落在 [from, to) 内
        // （与 ORC 按 stripe 起始偏移量归属的口径一致：每个 row group 的起始点唯一，
        // 相邻区间因此既不重也不漏）。
        //
        // 注意：parquet 的 HadoopReadOptions.withRange 只影响字节预取，并不会过滤 row group——
        // getRowGroups() 返回文件里**全部** row group，readNextRowGroup() 也会顺序读完所有 row group。
        // 所以不能一边遍历全部 row group 一边调 readNextRowGroup()：那样每个分片都会把整个文件读一遍，
        // 多分片时数据被重复 N 倍。这里按起始偏移量过滤后，用 readRowGroup(index)
        // 按绝对下标精确读取本区间内的那些 row group。
        var claimed = -1L
        val predicates = spec.predicates
        for ((index, block) in reader.rowGroups.withIndex()) {
            // 一个 row group 属于本区间，当且仅当它的起始偏移量落在 [from, to) 内
            // （与 ORC 按 stripe 起始偏移量归属的口径一致）。row group 按起始偏移量递增排列，
            // 越过区间末尾后就不必再遍历文件里剩下的 row group 了。
            if (block.startingPos >= range.to) {
                break
            }
            if (block.startingPos < range.from) {
                continue
            }
            // 谓词下推：用 row group 的列统计（min/max/null）判断整段不可能命中，直接跳过。
            // 没有谓词时走原快速路径，不碰列统计。
            if (predicates.isNotEmpty()) {
                val stats = collectParquetStats(block, fileSchema, predicates)
                if (PredicateEvaluator.canSkip(predicates, stats)) {
                    Metrics.counter(ParquetRecordReader::class.java, "parquetRowGroupsSkipped").inc()
                    continue
                }
            }
            if (block.startingPos > claimed) {
                if (!claim.tryClaim(block.startingPos)) {
                    return false
                }
                claimed = block.startingPos
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
     * 收集本 row group 里每个谓词列的 Parquet 列统计（min/max/null），用来判定整段是否可跳过。
     * 列类型不是数值/字符串、或该 row group 没有统计信息时，对应列返回 null（绝不跳过）。
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
            val ordinal = fileSchema.fields.indexOfFirst { it.name.equals(p.column, ignoreCase = true) }
            if (ordinal < 0 || ordinal >= columns.size) {
                continue
            }
            val stats = columns[ordinal].statistics
            // decimal 在 parquet 里存的是"未缩放值"，统计里也是未缩放的；换算回 BigDecimal 需要列的 scale。
            val scale = if (p.fieldType.typeName == Schema.TypeName.DECIMAL) {
                ((fileSchema.getType(ordinal) as? PrimitiveType)?.logicalTypeAnnotation as? LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)?.scale ?: 0
            } else {
                0
            }
            val (min, max) = extractParquetRange(stats, p.fieldType, scale)
            // parquet 1.17 没有 hasNull()，用 numNulls 反推；拿不到 numNulls 时按"可能有 null"处理（不跳过）。
            val hasNull = !(stats.isNumNullsSet && stats.numNulls == 0L)
            result[p.column] = ColumnRangeStats(min, max, hasNull)
        }
        return result
    }

    private fun extractParquetRange(
        stats: Statistics<*>,
        fieldType: Schema.FieldType,
        decimalScale: Int = 0,
    ): Pair<ValueRepr?, ValueRepr?> {
        return when (fieldType.typeName) {
            Schema.TypeName.BYTE, Schema.TypeName.INT16, Schema.TypeName.INT32, Schema.TypeName.INT64,
            Schema.TypeName.FLOAT, Schema.TypeName.DOUBLE -> {
                val mn = (stats.genericGetMin() as? Number)?.toDouble()
                val mx = (stats.genericGetMax() as? Number)?.toDouble()
                if (mn != null && mx != null) {
                    NumericValue(BigDecimal(mn)) to NumericValue(BigDecimal(mx))
                } else {
                    null to null
                }
            }

            Schema.TypeName.STRING -> {
                val mn = (stats.genericGetMin() as? Binary)?.toStringUsingUTF8()
                val mx = (stats.genericGetMax() as? Binary)?.toStringUsingUTF8()
                if (mn != null && mx != null) {
                    BytesValue(mn.toByteArray(StandardCharsets.UTF_8)) to
                        BytesValue(mx.toByteArray(StandardCharsets.UTF_8))
                } else {
                    null to null
                }
            }

            Schema.TypeName.DECIMAL -> {
                // parquet 1.17 没有 DecimalStatistics 这个类：precision<=9 走 INT32、<=18 走 INT64
                // （统计是未缩放的 Long/Integer），更大的 precision 走 FIXED_LEN_BYTE_ARRAY（统计是 big-endian
                // 未缩放字节）。两种都要按 scale 换回 BigDecimal 再比较，否则未缩放值和谓词值量纲不同会误判。
                val mn = decimalToBigDecimal(stats.genericGetMin(), decimalScale)
                val mx = decimalToBigDecimal(stats.genericGetMax(), decimalScale)
                if (mn != null && mx != null) {
                    NumericValue(mn) to NumericValue(mx)
                } else {
                    null to null
                }
            }

            else -> null to null
        }
    }

    private fun decimalToBigDecimal(raw: Any?, scale: Int): BigDecimal? = when (raw) {
        is Number -> BigDecimal(raw.toLong()).movePointLeft(scale)
        is Binary -> BigDecimal(BigInteger(raw.bytes), scale)
        else -> null
    }

    /**
     * 只读投影到的列。
     *
     * 按**列名**匹配（大小写不敏感）：Parquet 文件里的列名是 Hive 写进去的，和表定义一致；
     * 文件里没有的列（表后来加的列）直接不放进 requestedSchema，读出来是 null。
     */
    private fun project(fileSchema: MessageType): MessageType {
        val fields = spec.projectedDataColumns.mapNotNull { column ->
            fileSchema.fields.firstOrNull { it.name.equals(column.name, ignoreCase = true) }
        }
        if (fields.size < spec.projectedDataColumns.size) {
            LOGGER.info(
                "Parquet 文件[{}] 里缺少这些列，将读成 null: {}",
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
     * 标准的三层 LIST（`list` -> `element`）与 Hive 早年写出来的两层写法（`bag` -> `array_element`）都要认：
     * 中间那层永远是 `repeated`，元素是它下面唯一的字段；如果 repeated 那层直接是元素本身
     * （只有一个字段的 group 才算包装层），就把它当元素。
     */
    private fun readList(listGroup: Group, listType: GroupType, target: Schema.FieldType): List<Any?> {
        if (listType.fieldCount == 0) {
            return emptyList()
        }
        val repeatedType = listType.getType(0)
        val elementFieldType = target.collectionElementType!!
        val count = listGroup.getFieldRepetitionCount(0)
        if (repeatedType.isPrimitive) {
            // repeated 直接是元素（两层写法的一种）
            return (0 until count).map { i -> readPrimitive(listGroup, 0, i, repeatedType.asPrimitiveType(), elementFieldType.withNullable(false)) }
        }
        val repeatedGroup = repeatedType.asGroupType()
        if (repeatedGroup.fieldCount != 1) {
            // repeated 本身就是元素（struct 列表的两层写法）
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

            // Hive / Impala 早年用 int96 存 timestamp：前 8 字节是当天的纳秒数，后 4 字节是儒略日
            PrimitiveType.PrimitiveTypeName.INT96 ->
                int96ToInstant(group.getInt96(fieldIndex, valueIndex)).let {
                    if (target == me.jayer.hdata.core.type.FieldTypes.TIMESTAMP) it else LocalDateTime.ofInstant(it, ZoneOffset.UTC)
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

            else -> throw UnsupportedOperationException("暂不支持的 Parquet 类型: ${type.primitiveTypeName}")
        }
    }

    /** parquet 的整数只有 int32 / int64 两种，表上声明成 tinyint / smallint 时要窄回去。 */
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
        return if (target == me.jayer.hdata.core.type.FieldTypes.TIMESTAMP) {
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

        /** 儒略日 2440588 就是 1970-01-01。 */
        const val JULIAN_EPOCH_OFFSET_DAYS = 2_440_588L
    }
}
