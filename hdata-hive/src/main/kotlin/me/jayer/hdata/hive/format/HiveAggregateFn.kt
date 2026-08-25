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
import org.apache.orc.OrcFile
import org.apache.orc.Reader
import org.apache.orc.TypeDescription
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import java.io.Serializable
import java.math.BigDecimal

/**
 * 聚合下推（对标 Trino Hive 连接器的 aggregation pushdown）：
 * `count(*)` / `min(col)` / `max(col)` 直接读 ORC/Parquet 文件尾里的列统计，完全不扫行。
 *
 * ORC 用 `reader.numberOfRows` 与 `reader.getStatistics()` 里每列的文件级 min/max；
 * Parquet 用 `fileMetaData.blocks` 里每个 row group 的列统计，跨 row group 再归并一次。
 * 每个文件产出一行"部分聚合"，最后用 [AggregateCombineFn] 全局归并成唯一一行结果。
 *
 * 与谓词下推一样只支持数值（byte/short/int/long/float/double/decimal）与字符串列；
 * 其它类型或行式格式（TEXT/CSV/SEQ/RC/Avro）没有可用的列统计，无法下推，由配置校验显式拒绝。
 *
 * @author wuya
 */

/** 聚合函数类型。 */
enum class AggType {
    COUNT, MIN, MAX;

    companion object {
        fun of(name: String): AggType = entries.firstOrNull { it.name.equals(name.trim(), ignoreCase = true) }
            ?: throw IllegalArgumentException(
                "无法识别的 aggregate 类型: $name，可选: count / min / max"
            )
    }
}

/** 一个解析好的聚合请求；输出列名叫 `count` 或 `min_<列>` / `max_<列>`。 */
data class AggSpec(
    val type: AggType,
    /** MIN/MAX 的列名；COUNT 为 null。 */
    val column: String?,
    /** MIN/MAX 列在 Beam 里的类型；COUNT 用 INT64。 */
    val fieldType: Schema.FieldType,
    /** 输出字段名。 */
    val outputName: String,
) : Serializable

/** 聚合输出的 schema：每个聚合一项，COUNT 为 INT64，MIN/MAX 为列的原类型（可空）。 */
internal fun aggregateSchema(specs: List<AggSpec>): Schema {
    val builder = Schema.builder()
    specs.forEach { s ->
        when (s.type) {
            AggType.COUNT -> builder.addNullableField(s.outputName, FieldTypes.INT64)
            AggType.MIN, AggType.MAX -> builder.addNullableField(s.outputName, s.fieldType)
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

    @Setup
    fun setup() {
        schema = aggregateSchema(aggregates)
    }

    @ProcessElement
    fun processElement(@Element file: HiveFile, receiver: OutputReceiver<Row>) {
        val configuration = HiveFileSystems.configurationOf(hadoopConf)
        val format = HiveStorageFormat.of(file.partition.storage.storageFormat)
        val values = Array<Any?>(aggregates.size) { i ->
            val s = aggregates[i]
            when (s.type) {
                AggType.COUNT -> fileRowCount(format, file, configuration)
                AggType.MIN -> columnRange(format, file, s, configuration)?.first?.let { reprToValue(it, s.fieldType) }
                AggType.MAX -> columnRange(format, file, s, configuration)?.second?.let { reprToValue(it, s.fieldType) }
            }
        }
        receiver.output(Row.withSchema(schema).addValues(values.toList()).build())
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
 * 把每个文件的"部分聚合"一次性归并成最终结果：COUNT 求和，MIN 取跨文件最小，MAX 取跨文件最大。
 * 空表（没有文件）时 [partials] 为空，输出单位元：COUNT=0、MIN/MAX=null。
 *
 * 累加器本身是一个 schema 化的 [Row]，字段与 [aggregates] 一一对应：COUNT 为 INT64，
 * MIN/MAX 存"类型|原始值"的编码串（见 [encodeRepr]/[decodeRepr]），从而走 Beam 自带的 RowCoder，
 * 不需要自定义 Java 序列化。聚合下推里这部分数据量极小（每个文件一行），所以直接在一个 DoFn 内
 * 借助 side input 收集所有部分结果后归并，避开 Combine 内部 KV 的 coder 推断问题。
 */
fun mergeAggregatePartials(aggregates: List<AggSpec>, partials: List<Row>): Row {
    val schema: Schema = aggregateSchema(aggregates)
    val accumSchema: Schema = Schema.builder().let { b ->
        aggregates.forEachIndexed { i, a ->
            when (a.type) {
                AggType.COUNT -> b.addField("a$i", FieldTypes.INT64)
                AggType.MIN, AggType.MAX -> b.addNullableField("a$i", FieldTypes.STRING)
            }
        }
        b.build()
    }
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
                        AggType.MIN -> mergeRepr(acc.getValue<String?>(i), p.getValue<Any?>(i), aggregates[i].fieldType, takeMin = true)
                        AggType.MAX -> mergeRepr(acc.getValue<String?>(i), p.getValue<Any?>(i), aggregates[i].fieldType, takeMin = false)
                    }
                },
            )
            .build()
    }
    val values = List<Any?>(aggregates.size) { i ->
        when (aggregates[i].type) {
            AggType.COUNT -> acc.getValue<Long>(i)
            AggType.MIN, AggType.MAX -> {
                val enc = acc.getValue<String?>(i)
                if (enc == null) null else reprToValue(decodeRepr(enc), aggregates[i].fieldType)
            }
        }
    }
    return Row.withSchema(schema).addValues(values).build()
}

private fun mergeRepr(cur: String?, incomingRaw: Any?, fieldType: Schema.FieldType, takeMin: Boolean): String? {
    if (incomingRaw == null) return cur
    val incomingRepr = PredicateEvaluator.toRepr(incomingRaw, fieldType) ?: return cur
    val incEnc = encodeRepr(incomingRepr)
    if (cur == null) return incEnc
    val curRepr = decodeRepr(cur)
    val better = if (takeMin) incomingRepr.compareTo(curRepr) < 0 else incomingRepr.compareTo(curRepr) > 0
    return if (better) incEnc else cur
}

/** 把 [ValueRepr] 编码成"类型|原始值"形式的串，用于跨文件归并 MIN/MAX（累加器本身存字符串）。 */
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
