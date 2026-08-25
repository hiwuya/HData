package me.jayer.hdata.iceberg

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.Combine
import org.apache.beam.sdk.transforms.DoFn
import org.apache.iceberg.FileFormat
import org.apache.iceberg.FileScanTask
import org.apache.iceberg.Table
import org.apache.iceberg.data.Record
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Types

/**
 * Iceberg 聚合下推：COUNT / MIN / MAX 直接取自数据文件的元数据统计（recordCount / lower_bounds /
 * upper_bounds），根本不读数据文件——这是真正的存储层下推。AVRO 数据文件不存 SUM / AVG，
 * 所以这两项直接拒绝（收了又不生效等于埋坑），需要的话走读全量再聚合。
 *
 * 支持的聚合声明（`aggregations`）：
 * - `count` 或 `count:*`：COUNT(*)
 * - `min:<列>`：该列最小值
 * - `max:<列>`：该列最大值
 */
data class AggSpec(val op: String, val column: String?) : java.io.Serializable {
    init {
        require(op in setOf("count", "min", "max")) { "Iceberg 聚合只支持 count/min/max，不支持 '$op'（AVRO 数据文件不含 sum/avg 统计）" }
        require(op == "count" || column != null) { "$op 需要指定列" }
    }
}

fun parseAggregations(specs: List<String>): List<AggSpec> = specs.map { raw ->
    val (op, col) = raw.split(":", limit = 2).let { it[0].lowercase() to it.getOrNull(1) }
    AggSpec(op, col)
}

/** 聚合结果行的 schema：count → `count`(INT64)，min:age → `min_age`，max:age → `max_age`。 */
fun aggregateSchema(specs: List<AggSpec>, table: Table): Schema {
    val builder = Schema.builder()
    specs.forEach { (op, col) ->
        when (op) {
            "count" -> builder.addNullableField("count", Schema.FieldType.INT64)
            "min" -> builder.addNullableField("min_$col", typeToFieldType(table.schema().findField(col).type()))
            "max" -> builder.addNullableField("max_$col", typeToFieldType(table.schema().findField(col).type()))
        }
    }
    return builder.build()
}

/** 每个数据文件算出的局部聚合，作为合并单元在并行读与全局合并之间传递。 */
data class PartialAgg(
    val count: Long,
    val mins: Map<String, Comparable<*>?>,
    val maxs: Map<String, Comparable<*>?>,
) : java.io.Serializable

/**
 * 单个数据文件的局部聚合。COUNT 直接用文件元数据的 recordCount（不读数据）；
 * MIN/MAX 只投影对应的列、逐行取最值——比把整行都物化成 Beam Row 再聚合轻得多，
 * 是 Iceberg 不支持原生聚合下推时，往读取端推的做法（列投影下推）。
 * 注：Iceberg 1.10 的 InternalData.write 不会把列统计写进 manifest，所以 MIN/MAX 不能靠元数据，
 * 只能读投影列。
 */
fun partialAggFromTask(task: FileScanTask, specs: List<AggSpec>, table: Table): PartialAgg {
    val file = task.file()
    val schema = table.schema()
    val count = if (specs.any { it.op == "count" }) file.recordCount() else 0L
    val minMaxCols = specs.filter { it.op == "min" || it.op == "max" }.mapNotNull { it.column }.toSet()
    val mins = mutableMapOf<String, Comparable<*>?>()
    val maxs = mutableMapOf<String, Comparable<*>?>()
    if (minMaxCols.isNotEmpty()) {
        val dataColumns = schema.columns().filter { it.name() in minMaxCols }
        val projSchema = org.apache.iceberg.Schema(dataColumns)
        val inputFile = table.io().newInputFile(file.path().toString())
        val records = org.apache.iceberg.InternalData.read(FileFormat.AVRO, inputFile).project(projSchema).build<Record>()
        records.use { it.forEach { rec -> minMaxCols.forEach { col -> updateMinMax(rec, col, mins, maxs) } } }
    }
    return PartialAgg(count, mins, maxs)
}

private fun updateMinMax(rec: Record, col: String, mins: MutableMap<String, Comparable<*>?>, maxs: MutableMap<String, Comparable<*>?>) {
    val v = rec.getField(col) ?: return
    @Suppress("UNCHECKED_CAST")
    val c = (v as? Comparable<Any>) ?: return
    val curMin = mins[col]
    val curMax = maxs[col]
    if (curMin == null || (curMin as Comparable<Any>).compareTo(c) > 0) mins[col] = c
    if (curMax == null || (curMax as Comparable<Any>).compareTo(c) < 0) maxs[col] = c
}

/**
 * 把各文件的局部聚合合并成全局一个 [PartialAgg]（COUNT 求和，MIN/MAX 取跨文件最值）。
 * 输出仍是 [PartialAgg]，不直接出 Row——这样 Combine 内部的 coder 就是普通的 SerializableCoder，
 * 避免 Beam 在 Combine 内部推不出 Row coder；真正拼成 Row 交给下游的 [AggregateToRowFn]。
 */
class AggregateCombineFn(
    private val specs: List<AggSpec>,
) : Combine.CombineFn<PartialAgg, PartialAgg, PartialAgg>(), java.io.Serializable {

    override fun createAccumulator(): PartialAgg = PartialAgg(0, emptyMap(), emptyMap())

    override fun addInput(accumulator: PartialAgg, input: PartialAgg): PartialAgg {
        val mins = merge(accumulator.mins, input.mins, ::minOfSafe)
        val maxs = merge(accumulator.maxs, input.maxs, ::maxOfSafe)
        return PartialAgg(accumulator.count + input.count, mins, maxs)
    }

    override fun mergeAccumulators(accumulators: MutableIterable<PartialAgg>): PartialAgg {
        var acc = createAccumulator()
        accumulators.forEach { acc = addInput(acc, it) }
        return acc
    }

    override fun extractOutput(accumulator: PartialAgg): PartialAgg = accumulator

    private fun merge(
        a: Map<String, Comparable<*>?>,
        b: Map<String, Comparable<*>?>,
        pick: (Comparable<*>, Comparable<*>) -> Comparable<*>,
    ): Map<String, Comparable<*>?> {
        val keys = a.keys + b.keys
        val out = mutableMapOf<String, Comparable<*>?>()
        for (k in keys) {
            val va = a[k]
            val vb = b[k]
            out[k] = when {
                va == null -> vb
                vb == null -> va
                else -> pick(va, vb)
            }
        }
        return out
    }

    private fun minOfSafe(a: Comparable<*>, b: Comparable<*>): Comparable<*> =
        if ((a as Comparable<Any>).compareTo(b as Any) <= 0) a else b

    private fun maxOfSafe(a: Comparable<*>, b: Comparable<*>): Comparable<*> =
        if ((a as Comparable<Any>).compareTo(b as Any) >= 0) a else b
}

/**
 * 把合并后的 [PartialAgg] 拼成最终的一行 Row。单列在 [aggregateSchema] 里已定好类型，
 * 这里只按 specs 顺序取 count/min/max 的值填进 Row。
 */
class AggregateToRowFn(
    private val specs: List<AggSpec>,
    private val outputSchema: Schema,
) : DoFn<PartialAgg, org.apache.beam.sdk.values.Row>() {

    @ProcessElement
    fun processElement(@Element element: PartialAgg, receiver: OutputReceiver<org.apache.beam.sdk.values.Row>) {
        val builder = org.apache.beam.sdk.values.Row.withSchema(outputSchema)
        specs.forEach { (op, col) ->
            when (op) {
                "count" -> builder.addValue(element.count)
                "min" -> builder.addValue(element.mins[col])
                "max" -> builder.addValue(element.maxs[col])
            }
        }
        receiver.output(builder.build())
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/** Iceberg 基础类型 → Beam FieldType（聚合只支持标量，嵌套类型不在范围内）。 */
private fun typeToFieldType(type: org.apache.iceberg.types.Type): Schema.FieldType = when (type.typeId()) {
    Type.TypeID.LONG -> Schema.FieldType.INT64
    Type.TypeID.INTEGER -> Schema.FieldType.INT32
    Type.TypeID.STRING -> Schema.FieldType.STRING
    Type.TypeID.DOUBLE -> Schema.FieldType.DOUBLE
    Type.TypeID.FLOAT -> Schema.FieldType.FLOAT
    Type.TypeID.BOOLEAN -> Schema.FieldType.BOOLEAN
    else -> throw IllegalArgumentException("Iceberg 聚合不支持嵌套/非标量类型: $type")
}
