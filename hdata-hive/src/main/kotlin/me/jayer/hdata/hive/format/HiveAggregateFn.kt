package me.jayer.hdata.hive.format

import java.util.Base64
import me.jayer.hdata.core.type.FieldTypes
import me.jayer.hdata.hive.split.HiveFile
import me.jayer.hdata.hive.split.HiveFileSystems
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector
import org.apache.orc.OrcFile
import org.apache.orc.Reader
import org.apache.orc.TypeDescription
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import java.io.Serializable
import java.math.BigDecimal
import java.math.BigInteger
import java.math.RoundingMode

/**
 * 聚合下推（对标 Trino Hive 连接器的 aggregation pushdown）：
 * `count(*)` / `min(col)` / `max(col)` 直接读 ORC/Parquet 文件尾里的列统计，完全不扫行；
 * `sum(col)` / `avg(col)` 没有列统计可用，必须真的扫一遍文件累加成（sum, non-null count）。
 *
 * ORC 用 `reader.numberOfRows` 与 `reader.getStatistics()` 里每列的文件级 min/max；
 * Parquet 用 `fileMetaData.blocks` 里每个 row group 的列统计，跨 row group 再归并一次。
 * 每个文件产出一行"部分聚合"，最后用 [mergeAggregatePartials] 全局归并成唯一一行结果。
 *
 * 与谓词下推一样只支持数值（byte/short/int/long/float/double/decimal）与字符串列；
 * 其它类型或行式格式（TEXT/CSV/SEQ/RC/Avro）没有可用的列统计，无法下推，由配置校验显式拒绝。
 *
 * @author wuya
 */

/** 聚合函数类型。 */
enum class AggType {
    COUNT, MIN, MAX, SUM, AVG;

    companion object {
        fun of(name: String): AggType = entries.firstOrNull { it.name.equals(name.trim(), ignoreCase = true) }
            ?: throw IllegalArgumentException(
                "无法识别的 aggregate 类型: $name，可选: count / min / max / sum / avg"
            )
    }
}

/** 一个解析好的聚合请求；输出列名叫 `count` / `min_<列>` / `max_<列>` / `sum_<列>` / `avg_<列>`。 */
data class AggSpec(
    val type: AggType,
    /** MIN/MAX/SUM/AVG 的列名；COUNT 为 null。 */
    val column: String?,
    /** 列在 Beam 里的类型；COUNT 用 INT64。 */
    val fieldType: Schema.FieldType,
    /** 输出字段名。 */
    val outputName: String,
    /** AVG 对 DECIMAL 列求均值时保持的标度（来自列声明）；非 DECIMAL 为 0。 */
    val decimalScale: Int = 0,
) : Serializable

/** AVG 的输出类型：DECIMAL 列保持 DECIMAL，其它数值列统一出 DOUBLE（均值可能非整数）。 */
private fun avgOutputType(fieldType: Schema.FieldType): Schema.FieldType = when (fieldType.typeName) {
    Schema.TypeName.DECIMAL -> FieldTypes.DECIMAL
    else -> FieldTypes.DOUBLE
}

/** 聚合输出的 schema：每个聚合一项，COUNT 为 INT64，MIN/MAX/SUM 为列的原类型，AVG 为数值/DECIMAL（可空）。 */
internal fun aggregateSchema(specs: List<AggSpec>): Schema {
    val builder = Schema.builder()
    specs.forEach { s ->
        when (s.type) {
            AggType.COUNT -> builder.addNullableField(s.outputName, FieldTypes.INT64)
            AggType.MIN, AggType.MAX, AggType.SUM -> builder.addNullableField(s.outputName, s.fieldType)
            AggType.AVG -> builder.addNullableField(s.outputName, avgOutputType(s.fieldType))
        }
    }
    return builder.build()
}

/**
 * 部分聚合（每个文件一行）的 schema：COUNT 为 INT64，MIN/MAX/SUM/AVG 全部存成编码串，
 * 这样既能走 Beam 自带的 RowCoder，又能和 [mergeAggregatePartials] 里的累加器保持一致。
 * 输出 schema 见 [aggregateSchema]，两者只有 SUM/AVG 的字段类型不同。
 */
internal fun aggregateAccumSchema(specs: List<AggSpec>): Schema {
    val builder = Schema.builder()
    specs.forEach { s ->
        when (s.type) {
            AggType.COUNT -> builder.addNullableField(s.outputName, FieldTypes.INT64)
            AggType.MIN, AggType.MAX, AggType.SUM, AggType.AVG -> builder.addNullableField(s.outputName, FieldTypes.STRING)
        }
    }
    return builder.build()
}

/**
 * 每个文件产出一行"部分聚合"：COUNT 是该文件的行数，MIN/MAX 是该文件列统计里的 min/max（或 null）。
 * 不扫任何行，只读文件尾。
 */
class HiveAggregateFn(
    private val aggregates: List<AggSpec>,
    private val hadoopConf: Map<String, String>,
) : DoFn<HiveFile, Row>() {

    @Transient private lateinit var schema: Schema
    @Transient private lateinit var accumSchema: Schema

    @Setup
    fun setup() {
        schema = aggregateSchema(aggregates)
        accumSchema = aggregateAccumSchema(aggregates)
    }

    @ProcessElement
    fun processElement(@Element file: HiveFile, receiver: OutputReceiver<Row>) {
        val configuration = HiveFileSystems.configurationOf(hadoopConf)
        val format = HiveStorageFormat.of(file.partition.storage.storageFormat)
        // SUM / AVG 没有列统计可用，必须真的扫文件累加成 (sum, count)；整文件只扫这一次。
        val sumAvgSpecs = aggregates.filter { it.type == AggType.SUM || it.type == AggType.AVG }
        val scanned = if (sumAvgSpecs.isNotEmpty()) scanSums(format, file, configuration, sumAvgSpecs) else emptyMap()
        val values = Array<Any?>(aggregates.size) { i ->
            val s = aggregates[i]
            when (s.type) {
                AggType.COUNT -> fileRowCount(format, file, configuration)
                AggType.MIN -> columnRange(format, file, s, configuration)?.first?.let { encodeRepr(it) }
                AggType.MAX -> columnRange(format, file, s, configuration)?.second?.let { encodeRepr(it) }
                AggType.SUM -> {
                    val (sum, _) = scanned[s] ?: (BigDecimal.ZERO to 0L)
                    encodeRepr(NumericValue(sum))
                }
                AggType.AVG -> {
                    val (sum, cnt) = scanned[s] ?: (BigDecimal.ZERO to 0L)
                    if (cnt == 0L) null else encodeAvg(NumericValue(sum), cnt)
                }
            }
        }
        receiver.output(Row.withSchema(accumSchema).addValues(values.toList()).build())
    }

    /** 扫整个文件，对每个 SUM/AVG 列累加出 (sum, count)。COUNT/MIN/MAX 不在这里算。 */
    private fun scanSums(
        format: HiveStorageFormat,
        file: HiveFile,
        configuration: Configuration,
        specs: List<AggSpec>,
    ): Map<AggSpec, Pair<BigDecimal, Long>> {
        val result = specs.associateWith { BigDecimal.ZERO to 0L }.toMutableMap()
        when (format) {
            HiveStorageFormat.ORC -> scanOrcSums(file, configuration, specs, result)
            HiveStorageFormat.PARQUET -> scanParquetSums(file, configuration, specs, result)
            else -> throw UnsupportedOperationException("聚合下推仅支持 ORC / Parquet，遇到 $format")
        }
        return result
    }

    private fun scanOrcSums(
        file: HiveFile,
        configuration: Configuration,
        specs: List<AggSpec>,
        result: MutableMap<AggSpec, Pair<BigDecimal, Long>>,
    ) {
        val path = Path(file.path)
        OrcFile.createReader(
            path,
            OrcFile.readerOptions(configuration).filesystem(path.getFileSystem(configuration)).useUTCTimestamp(true),
        ).use { reader ->
            val schema = reader.schema
            val colIds = specs.mapNotNull { s -> orcColumnId(schema, s.column!!)?.let { s to it } }.toMap()
            if (colIds.isEmpty()) return
            // 只读 SUM/AVG 涉及的列，减少解压量
            val include = BooleanArray(schema.maximumId + 1)
            include[0] = true
            colIds.values.forEach { id -> for (x in id..schema.findSubtype(id).maximumId) include[x] = true }
            val batch = schema.createRowBatch()
            val rows = reader.rows(reader.options().include(include))
            while (rows.nextBatch(batch)) {
                for ((s, id) in colIds) {
                    val col = batch.cols[id - 1] ?: continue
                    val (sum, cnt) = result[s]!!
                    var newSum = sum
                    var newCnt = cnt
                    for (rowIndex in 0 until batch.size) {
                        val v = orcNumericValue(col, rowIndex, schema.findSubtype(id)) ?: continue
                        newSum = newSum.add(v)
                        newCnt++
                    }
                    result[s] = newSum to newCnt
                }
            }
        }
    }

    private fun scanParquetSums(
        file: HiveFile,
        configuration: Configuration,
        specs: List<AggSpec>,
        result: MutableMap<AggSpec, Pair<BigDecimal, Long>>,
    ) {
        val path = Path(file.path)
        val fileSchema = ParquetFileReader.open(
            HadoopInputFile.fromPath(path, configuration),
            HadoopReadOptions.builder(configuration, path).build(),
        ).use { it.footer.fileMetaData.schema }
        val scaleByCol = specs.associate {
            it.column!! to parquetDecimalScale(fileSchema, fileSchema.fields.indexOfFirst { f -> f.name.equals(it.column, ignoreCase = true) })
        }
        ParquetReader.builder(GroupReadSupport(), path).withConf(configuration).build().use { reader ->
            var group = reader.read()
            while (group != null) {
                for (s in specs) {
                    val col = s.column!!
                    val ordinal = fileSchema.fields.indexOfFirst { f -> f.name.equals(col, ignoreCase = true) }
                    if (ordinal < 0) continue
                    if (group.getFieldRepetitionCount(ordinal) == 0) continue
                    val v = when (s.fieldType.typeName) {
                        Schema.TypeName.INT64 -> group.getLong(ordinal, 0)
                        Schema.TypeName.INT32, Schema.TypeName.INT16, Schema.TypeName.BYTE -> group.getInteger(ordinal, 0).toLong()
                        Schema.TypeName.DOUBLE -> group.getDouble(ordinal, 0)
                        Schema.TypeName.FLOAT -> group.getFloat(ordinal, 0).toDouble()
                        Schema.TypeName.DECIMAL -> {
                            val binary = group.getBinary(ordinal, 0)
                            BigDecimal(BigInteger(binary.bytes), scaleByCol[col]!!)
                        }
                        else -> null
                    } ?: continue
                    val r = NumericValue(BigDecimal(v.toString()))
                    val (sum, cnt) = result[s]!!
                    result[s] = sum.add(r.v) to cnt + 1
                }
                group = reader.read()
            }
        }
    }

    /** 从 ORC 向量里取一个数值单元格（SUM/AVG 只需要数值列）。null 或类型不支持返回 null。 */
    private fun orcNumericValue(col: ColumnVector, rowIndex: Int, type: TypeDescription): BigDecimal? {
        val index = if (col.isRepeating) 0 else rowIndex
        if (!col.noNulls && col.isNull[index]) return null
        return when (type.category) {
            TypeDescription.Category.INT,
            TypeDescription.Category.BYTE,
            TypeDescription.Category.SHORT,
            TypeDescription.Category.LONG,
            -> BigDecimal((col as LongColumnVector).vector[index])

            TypeDescription.Category.FLOAT,
            TypeDescription.Category.DOUBLE,
            -> BigDecimal((col as DoubleColumnVector).vector[index].toDouble())

            TypeDescription.Category.DECIMAL ->
                (col as DecimalColumnVector).vector[index].hiveDecimal.bigDecimalValue()
                    .setScale(type.scale, RoundingMode.HALF_UP)

            else -> null
        }
    }

    private fun fileRowCount(format: HiveStorageFormat, file: HiveFile, configuration: Configuration): Long =
        when (format) {
            HiveStorageFormat.ORC -> {
                val path = Path(file.path)
                OrcFile.createReader(
                    path,
                    OrcFile.readerOptions(configuration).filesystem(path.getFileSystem(configuration)).useUTCTimestamp(true),
                ).use { it.numberOfRows }
            }

            HiveStorageFormat.PARQUET -> {
                val path = Path(file.path)
                ParquetFileReader.open(HadoopInputFile.fromPath(path, configuration), HadoopReadOptions.builder(configuration, path).build())
                    .use { it.rowGroups.sumOf { b -> b.rowCount } }
            }

            else -> throw UnsupportedOperationException("聚合下推仅支持 ORC / Parquet，遇到 $format")
        }

    /** 取某列在本文件里的 (min, max) 可比较表示；拿不到统计返回 null。 */
    private fun columnRange(
        format: HiveStorageFormat,
        file: HiveFile,
        spec: AggSpec,
        configuration: Configuration,
    ): Pair<ValueRepr?, ValueRepr?>? {
        val column = spec.column ?: return null
        return when (format) {
            HiveStorageFormat.ORC -> {
                val path = Path(file.path)
                OrcFile.createReader(
                    path,
                    OrcFile.readerOptions(configuration).filesystem(path.getFileSystem(configuration)).useUTCTimestamp(true),
                ).use { reader ->
                    val colId = orcColumnId(reader.schema, column) ?: return null
                    val type = reader.schema.findSubtype(colId)
                    val scale = if (type.category == TypeDescription.Category.DECIMAL) type.scale else 0
                    val stats = reader.getStatistics()
                    if (colId >= stats.size) return null
                    orcColumnRange(stats[colId], spec.fieldType, scale)
                }
            }

            HiveStorageFormat.PARQUET -> {
                val path = Path(file.path)
                ParquetFileReader.open(HadoopInputFile.fromPath(path, configuration), HadoopReadOptions.builder(configuration, path).build())
                    .use { reader ->
                        val fileSchema: MessageType = reader.footer.fileMetaData.schema
                        val ordinal = fileSchema.fields.indexOfFirst { it.name.equals(column, ignoreCase = true) }
                        if (ordinal < 0 || ordinal >= fileSchema.columns.size) return null
                        val scale = parquetDecimalScale(fileSchema, ordinal)
                        var minR: ValueRepr? = null
                        var maxR: ValueRepr? = null
                        for (block in reader.rowGroups) {
                            val colStats = block.columns[ordinal].statistics
                            if (!colStats.hasNonNullValue()) continue
                            val (mn, mx) = parquetColumnRange(colStats, spec.fieldType, scale)
                            if (mn != null) minR = if (minR == null || mn.compareTo(minR) < 0) mn else minR
                            if (mx != null) maxR = if (maxR == null || mx.compareTo(maxR) > 0) mx else maxR
                        }
                        minR to maxR
                    }
            }

            else -> throw UnsupportedOperationException("聚合下推仅支持 ORC / Parquet，遇到 $format")
        }
    }

    private fun orcColumnId(schema: TypeDescription, column: String): Int? {
        val byName = schema.fieldNames.indexOfFirst { it.equals(column, ignoreCase = true) }
        return if (byName >= 0) byName + 1 else null
    }

    private fun parquetDecimalScale(fileSchema: MessageType, ordinal: Int): Int {
        val type = fileSchema.getType(ordinal)
        return (type.logicalTypeAnnotation as? LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)?.scale ?: 0
    }
}

/**
 * 把每个文件的"部分聚合"一次性归并成最终结果：
 *  - COUNT 求和；MIN/MAX 取跨文件极值（只靠列统计、不扫行）；
 *  - SUM 累加各文件 sum；AVG = 跨文件 sum / 跨文件 non-null count。
 *
 * 空表（没有文件）时 [partials] 为空，输出单位元：COUNT=0、MIN/MAX/SUM=null、AVG=null。
 *
 * 累加器本身是一个 schema 化的 [Row]，字段与 [aggregates] 一一对应：COUNT 为 INT64，
 * MIN/MAX/SUM/AVG 存编码串（MIN/MAX/SUM 用 [encodeRepr]，AVG 用 [encodeAvg]），从而走 Beam 自带的
 * RowCoder，不需要自定义 Java 序列化。聚合下推里这部分数据量极小（每个文件一行），所以直接在一个 DoFn
 * 内借助 side input 收集所有部分结果后归并，避开 Combine 内部 KV 的 coder 推断问题。
 */
fun mergeAggregatePartials(aggregates: List<AggSpec>, partials: List<Row>): Row {
    val schema: Schema = aggregateSchema(aggregates)
    val accumSchema: Schema = aggregateAccumSchema(aggregates)
    var acc = Row.withSchema(accumSchema)
        .addValues(
            List(aggregates.size) { i ->
                if (aggregates[i].type == AggType.COUNT) 0L else null
            },
        )
        .build()
    for (p in partials) {
        acc = Row.withSchema(accumSchema)
            .addValues(
                List(aggregates.size) { i ->
                    when (aggregates[i].type) {
                        AggType.COUNT -> (acc.getValue<Long>(i) ?: 0L) + (p.getValue<Long>(i) ?: 0L)
                        AggType.MIN -> mergeRepr(acc.getValue<String?>(i), p.getValue<String?>(i), aggregates[i].fieldType, takeMin = true)
                        AggType.MAX -> mergeRepr(acc.getValue<String?>(i), p.getValue<String?>(i), aggregates[i].fieldType, takeMin = false)
                        AggType.SUM -> mergeSum(acc.getValue<String?>(i), p.getValue<String?>(i))
                        AggType.AVG -> mergeAvg(acc.getValue<String?>(i), p.getValue<String?>(i))
                    }
                },
            )
            .build()
    }
    val values = List<Any?>(aggregates.size) { i ->
        when (aggregates[i].type) {
            AggType.COUNT -> acc.getValue<Long>(i)
            AggType.MIN, AggType.MAX, AggType.SUM -> {
                val enc = acc.getValue<String?>(i)
                if (enc == null) null else reprToValue(decodeRepr(enc), aggregates[i].fieldType)
            }
            AggType.AVG -> {
                val enc = acc.getValue<String?>(i)
                if (enc == null) null else {
                    val (sR, c) = decodeAvg(enc)
                    avgValue(sR, c, aggregates[i].decimalScale)
                }
            }
        }
    }
    return Row.withSchema(schema).addValues(values).build()
}

private fun mergeRepr(cur: String?, incoming: String?, fieldType: Schema.FieldType, takeMin: Boolean): String? {
    if (incoming == null) return cur
    if (cur == null) return incoming
    val curRepr = decodeRepr(cur)
    val incRepr = decodeRepr(incoming)
    val better = if (takeMin) incRepr.compareTo(curRepr) < 0 else incRepr.compareTo(curRepr) > 0
    return if (better) incoming else cur
}

private fun mergeSum(cur: String?, incoming: String?): String? {
    if (incoming == null) return cur
    if (cur == null) return incoming
    val curR = decodeRepr(cur) as? NumericValue ?: return incoming
    val incR = decodeRepr(incoming) as? NumericValue ?: return cur
    return encodeRepr(NumericValue(curR.v.add(incR.v)))
}

private fun mergeAvg(cur: String?, incoming: String?): String? {
    if (incoming == null) return cur
    if (cur == null) return incoming
    val (s1, c1) = decodeAvg(cur)
    val (s2, c2) = decodeAvg(incoming)
    val s1n = (s1 as? NumericValue)?.v ?: return incoming
    val s2n = (s2 as? NumericValue)?.v ?: return cur
    return encodeAvg(NumericValue(s1n.add(s2n)), c1 + c2)
}

/** AVG 的跨文件归并中间表示：sum（数值 repr）与 non-null 行数，用 `#` 分隔避免与 [encodeRepr] 的 `|` 冲突。 */
private fun encodeAvg(sumRepr: ValueRepr, count: Long): String = "A#${encodeRepr(sumRepr)}#$count"

private fun decodeAvg(s: String): Pair<ValueRepr, Long> {
    val parts = s.split("#")
    // parts = ["A", "<numEnc>", "<count>"]
    return decodeRepr(parts[1]) to parts[2].toLong()
}

private fun avgValue(sumRepr: ValueRepr, count: Long, decimalScale: Int): Any? {
    if (count == 0L) return null
    val sum = (sumRepr as NumericValue).v
    val avg = sum.divide(BigDecimal(count), 12, RoundingMode.HALF_UP)
    return if (decimalScale > 0) avg.setScale(decimalScale, RoundingMode.HALF_UP) else avg.toDouble()
}

/** 把 [ValueRepr] 编码成"类型|原始值"形式的串，用于跨文件归并 MIN/MAX/SUM（累加器本身存字符串）。 */
fun encodeRepr(r: ValueRepr): String = when (r) {
    is NumericValue -> "N|${r.v}"
    is BytesValue -> "B|" + Base64.getEncoder().encodeToString(r.v)
}

fun decodeRepr(s: String): ValueRepr {
    val sep = s.indexOf('|')
    val kind = s.substring(0, sep)
    val raw = s.substring(sep + 1)
    return when (kind) {
        "N" -> NumericValue(BigDecimal(raw))
        "B" -> BytesValue(Base64.getDecoder().decode(raw))
        else -> throw IllegalArgumentException("无法解码聚合代表值: $s")
    }
}
