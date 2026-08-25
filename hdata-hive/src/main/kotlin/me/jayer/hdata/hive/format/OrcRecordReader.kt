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
import java.util.Random
import me.jayer.hdata.hive.SampleMethod
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset

/**
 * ORC 读取器，直接用 orc-core 的向量化接口读，不经过 Hive 的 `OrcSerde` / `OrcInputFormat`。
 *
 * 可认领的边界是 **stripe**：ORC 的文件尾里记着每个 stripe 的偏移量和长度，
 * 落在本区间内的 stripe 逐个认领、逐个读。这样运行时才能在读到一半时把剩下的 stripe
 * 分给空闲 worker——把整段一次认领掉就没有这个可能了。
 *
 * 时间戳统一按 UTC 读（`useUTCTimestamp(true)`）。ORC 的 TIMESTAMP 默认会做
 * "写入方时区 -> 读取方时区"的换算，同一个文件在不同时区的机器上读出来的墙上时间会不一样，
 * 对同步工具来说这是必须消掉的不确定性；写入端也开同一个开关，两边才对得上。
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
        // 整块采样（SYSTEM）：每个 stripe 以 fraction 概率被整段跳过，IO 直接省掉（对标 Trino 的 TABLESAMPLE SYSTEM）。
        val doSystemSample = spec.sampleMethod == SampleMethod.SYSTEM && spec.sampleFraction < 1.0
        val systemRng = if (doSystemSample) Random(spec.sampleSeed ?: System.nanoTime()) else null
        // 有谓词时才一次性读全部 stripe 统计；没有谓词时完全不碰统计，保持原快速路径。
        val stripeStats = if (predicates.isEmpty()) emptyList() else orcReader.stripeStatistics
        for ((i, stripe) in orcReader.stripes.withIndex()) {
            // stripe 的归属按起始偏移量算，和 Trino / Hive 的切分口径一致：
            // 落在本区间内的 stripe 逐个认领、逐个读。stripe 按偏移量递增排列，
            // 越过区间末尾后就不必再遍历文件里剩下的 stripe 了。
            if (stripe.offset >= range.to) {
                break
            }
            if (stripe.offset < range.from) {
                continue
            }
            // 整块采样：本 stripe 被抽中"丢弃"就直接跳过，不读它。
            if (doSystemSample && systemRng!!.nextDouble() >= spec.sampleFraction) {
                Metrics.counter(OrcRecordReader::class.java, "orcStripesSkipped").inc()
                continue
            }
            // 谓词下推：用 stripe 的列统计（min/max/null）判断整段不可能命中，直接跳过。
            if (predicates.isNotEmpty()) {
                val stats = collectOrcStats(fileSchema, stripeStats[i], predicates)
                if (PredicateEvaluator.canSkip(predicates, stats)) {
                    Metrics.counter(OrcRecordReader::class.java, "orcStripesSkipped").inc()
                    continue
                }
            }
            if (!claim.tryClaim(stripe.offset)) {
                return false
            }
            readStripe(orcReader, stripe.offset, stripe.length, include, fileSchema, mapping, fieldTypes, output)
        }
        return true
    }

    /**
     * 收集本 stripe 里每个谓词列的 ORC 列统计（min/max/null），用来判定整段是否可跳过。
     * 列类型不是数值/字符串、或该 stripe 没有统计信息时，对应列返回 null（绝不跳过）。
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
            val id = orcColumnId(fileSchema, p.column) ?: continue
            if (id >= columns.size) {
                continue
            }
            val colStats = columns[id]
            val (min, max) = extractOrcRange(colStats, p.fieldType)
            // ORC 的 ColumnStatistics 只暴露 hasNull（是否有 NULL），没有 null 计数，无法证明"整列全 NULL"，
            // 所以 IS NOT NULL 的整段跳过对 ORC 保守地不触发（allNull=false），只靠行级过滤兜底。
            result[p.column] = ColumnRangeStats(min, max, colStats.hasNull(), false)
        }
        return result
    }

    /** 谓词列名 → ORC 列 id（从 1 开始）。先按名匹配，再退回到按数据列顺序，拿不到就跳过。 */
    private fun orcColumnId(fileSchema: TypeDescription, column: String): Int? {
        val byName = fileSchema.fieldNames.indexOfFirst { it.equals(column, ignoreCase = true) }
        if (byName >= 0) {
            return byName + 1
        }
        val dataIndex = spec.dataColumns.indexOfFirst { it.name.equals(column, ignoreCase = true) }
        return if (dataIndex >= 0) dataIndex + 1 else null
    }

    private fun extractOrcRange(
        colStats: ColumnStatistics,
        fieldType: Schema.FieldType,
    ): Pair<ValueRepr?, ValueRepr?> {
        return when (fieldType.typeName) {
            Schema.TypeName.BYTE, Schema.TypeName.INT16, Schema.TypeName.INT32, Schema.TypeName.INT64 -> {
                if (colStats is IntegerColumnStatistics) {
                    NumericValue(BigDecimal(colStats.minimum)) to NumericValue(BigDecimal(colStats.maximum))
                } else {
                    null to null
                }
            }

            Schema.TypeName.FLOAT, Schema.TypeName.DOUBLE -> {
                if (colStats is DoubleColumnStatistics) {
                    NumericValue(BigDecimal(colStats.minimum)) to NumericValue(BigDecimal(colStats.maximum))
                } else {
                    null to null
                }
            }

            Schema.TypeName.DECIMAL -> {
                // ORC 的 DecimalColumnStatistics 直接给出 BigDecimal，无需按 scale 换算。
                if (colStats is DecimalColumnStatistics) {
                    val mn = colStats.minimum?.bigDecimalValue()
                    val mx = colStats.maximum?.bigDecimalValue()
                    if (mn != null && mx != null) {
                        NumericValue(mn) to NumericValue(mx)
                    } else {
                        null to null
                    }
                } else {
                    null to null
                }
            }

            Schema.TypeName.STRING -> {
                if (colStats is StringColumnStatistics) {
                    BytesValue(colStats.minimum.toByteArray(StandardCharsets.UTF_8)) to
                        BytesValue(colStats.maximum.toByteArray(StandardCharsets.UTF_8))
                } else {
                    null to null
                }
            }

            else -> null to null
        }
    }

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
     * 投影到的每一列在 ORC 文件里的下标，-1 表示文件里没有这一列（加列之后的老文件）。
     *
     * 优先按**列名**匹配；文件里的列名对不上（Hive 早年写出来的 ORC 里字段就叫 `_col0`、`_col1`）
     * 时退回按**位置**匹配。Trino 用 `hive.orc.use-column-names` 这个开关让用户自己选，
     * 这里改成自动判断：全部投影列都能按名字找到才用名字，否则一律按位置。
     */
    private fun resolveColumns(fileSchema: TypeDescription): IntArray {
        require(fileSchema.category == TypeDescription.Category.STRUCT) {
            "ORC 文件的顶层类型应该是 struct，实际是 ${fileSchema.category}: ${file.path}"
        }
        val fileNames = fileSchema.fieldNames
        val byName = spec.projectedDataColumns.map { column ->
            fileNames.indexOfFirst { it.equals(column.name, ignoreCase = true) }
        }
        if (byName.none { it < 0 }) {
            return byName.toIntArray()
        }
        LOGGER.info(
            "ORC 文件[{}] 的列名与表定义对不上（文件里是 {}），改按列的位置匹配",
            file.path,
            fileNames.take(8),
        )
        return spec.projectedDataIndexes.map { if (it < fileSchema.children.size) it else -1 }.toIntArray()
    }

    /** 只读投影到的列。ORC 的 include 数组按 [TypeDescription.getId] 索引，要把子树整棵标上。 */
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
        // isRepeating 的向量只有第 0 个位置有值
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

            // HiveDecimal 会把末尾的 0 抹掉：decimal(10,2) 里的 4.50 读出来是标度 1 的 4.5，
            // 数值相等但 equals 不成立。按列声明的标度补回去
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

            else -> throw UnsupportedOperationException("暂不支持的 ORC 类型: ${type.category}（列 ${type.id}）")
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
