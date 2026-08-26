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
 * Iceberg 聚合下推：COUNT 直接取自数据文件的元数据 `recordCount`（不读数据）；
 * MIN / MAX / SUM / AVG 只投影对应的列、逐文件扫描累加，再跨文件全局归并——
 * 是 Iceberg 不支持原生聚合下推时，往读取端推的做法（列投影下推）。
 *
 * 注：Iceberg 1.10 的 `InternalData.write` 不会把列统计写进 manifest，所以 MIN/MAX 不能靠元数据，
 * 只能读投影列；SUM/AVG 同理（AVRO 数据文件本就不含这两项统计），但都可以在读取端按列累加得到正确结果。
 *
 * 支持的聚合声明（`aggregations`）：
 * - `count` 或 `count:*`：COUNT(*)
 * - `min:<列>` / `max:<列>`：该列最值（结果类型与列一致）
 * - `sum:<列>` / `avg:<列>`：该列求和 / 均值（结果统一为 DOUBLE，与 JDBC/ES 一致）
 */
data class AggSpec(val op: String, val column: String?) : java.io.Serializable {
    init {
        require(op in setOf("count", "min", "max", "sum", "avg")) {
            "Iceberg 聚合只支持 count/min/max/sum/avg，不支持 '$op'"
        }
        require(op == "count" || column != null) { "$op 需要指定列" }
    }
}

fun parseAggregations(specs: List<String>): List<AggSpec> = specs.map { raw ->
    val (op, col) = raw.split(":", limit = 2).let { it[0].lowercase() to it.getOrNull(1) }
    AggSpec(op, col)
}.also { parsed ->
    // 输出列名重复会让 Beam Schema 直接报难懂的错，这里提前给清楚的信息
    val names = parsed.map { aggOutputName(it) }
    require(names.distinct().size == names.size) { "aggregations 输出列名重复: ${names.joinToString()}" }
}

/** 聚合结果行的字段名：count / min_<列> / max_<列> / sum_<列> / avg_<列>。 */
fun aggOutputName(spec: AggSpec): String =
    if (spec.op == "count") "count" else "${spec.op}_${spec.column}"

/** 聚合结果行的 schema：count → `count`(INT64)；min/max 与列同类型；sum/avg → DOUBLE。 */
fun aggregateSchema(specs: List<AggSpec>, table: Table): Schema {
    val builder = Schema.builder()
    specs.forEach { (op, col) ->
        when (op) {
            "count" -> builder.addNullableField("count", Schema.FieldType.INT64)
            "min" -> builder.addNullableField("min_$col", typeToFieldType(findColumn(table, col).type()))
            "max" -> builder.addNullableField("max_$col", typeToFieldType(findColumn(table, col).type()))
            "sum", "avg" -> {
                val type = findColumn(table, col).type()
                require(type.typeId() in setOf(Type.TypeID.INTEGER, Type.TypeID.LONG, Type.TypeID.FLOAT, Type.TypeID.DOUBLE)) {
                    "Iceberg 聚合 $op 只支持数值列，列[$col] 的类型是 $type"
                }
                builder.addNullableField("${op}_$col", Schema.FieldType.DOUBLE)
            }
        }
    }
    return builder.build()
}

private fun findColumn(table: Table, col: String?) =
    requireNotNull(table.schema().findField(col)) { "聚合列[$col] 在表[${table.name()}] 中不存在" }

/**
 * 每个数据文件算出的局部聚合，作为合并单元在并行读与全局合并之间传递。
 * `sums` / `nonNull` 按列名聚合（SUM 与 AVG 共用同一份列累加，AVG 用 `sums/count` 还原）。
 */
data class PartialAgg(
    val count: Long = 0,
    val mins: Map<String, Comparable<*>?> = emptyMap(),
    val maxs: Map<String, Comparable<*>?> = emptyMap(),
    val sums: Map<String, Double> = emptyMap(),
    val nonNull: Map<String, Long> = emptyMap(),
) : java.io.Serializable

/**
 * 单个数据文件的局部聚合。
 *
 * 不带 filter 时：COUNT 直接用文件元数据的 recordCount（不读数据）；MIN/MAX/SUM/AVG 只投影对应的列、
 * 逐行累加——比把整行都物化成 Beam Row 再聚合轻得多。注：Iceberg 1.10 的 InternalData.write 不会把
 * 列统计写进 manifest，所以 MIN/MAX 也不能靠元数据，只能读投影列。
 *
 * 带 filter 时（[filterEvaluator] 非空）：COUNT 不能再用 recordCount——那是整文件的行数，含不匹配的行；
 * 必须逐行求值残留谓词后计数，MIN/MAX/SUM/AVG 也只累计匹配的行。此时投影**全表所有列**
 * （谓词可能引用聚合之外的列），代价是多读几列，换来的是结果正确。
 */
fun partialAggFromTask(
    task: FileScanTask,
    specs: List<AggSpec>,
    table: Table,
    filterEvaluator: org.apache.iceberg.expressions.Evaluator? = null,
): PartialAgg {
    val file = task.file()
    val schema = table.schema()
    val hasCount = specs.any { it.op == "count" }
    val numCols = specs.filter { it.op in setOf("min", "max", "sum", "avg") }.mapNotNull { it.column }.toSet()

    // 纯 COUNT 且无 filter：文件元数据直接给答案，一个字节都不用读
    if (filterEvaluator == null && !hasCount && numCols.isEmpty()) {
        return PartialAgg()
    }

    val mins = mutableMapOf<String, Comparable<*>?>()
    val maxs = mutableMapOf<String, Comparable<*>?>()
    val colSum = mutableMapOf<String, Double>()
    val colCount = mutableMapOf<String, Long>()
    var matched = 0L

    if (filterEvaluator == null && numCols.isEmpty()) {
        matched = file.recordCount()
    } else {
        // 带 filter 时谓词可能引用任意列，投影全表列；否则只投影聚合涉及的列
        val colsToRead = if (filterEvaluator != null) schema.columns().map { it.name() }.toSet() else numCols
        val dataColumns = schema.columns().filter { it.name() in colsToRead }
        val projSchema = org.apache.iceberg.Schema(dataColumns)
        val inputFile = table.io().newInputFile(file.path().toString())
        val records = org.apache.iceberg.InternalData.read(FileFormat.AVRO, inputFile).project(projSchema).build<Record>()
        records.use {
            it.forEach { rec ->
                if (filterEvaluator != null && !filterEvaluator.eval(rec)) return@forEach
                matched++
                numCols.forEach { col -> updateColumn(rec, col, mins, maxs, colSum, colCount) }
            }
        }
    }

    val count = when {
        filterEvaluator != null -> if (hasCount) matched else 0L
        hasCount -> file.recordCount()
        else -> 0L
    }
    // 只有真正声明了 sum/avg 的列才需要带 sums/nonNull 下去，避免无谓的 Map 传输
    val sumCols = specs.filter { it.op == "sum" || it.op == "avg" }.mapNotNull { it.column }.toSet()
    val sums = colSum.filterKeys { it in sumCols }
    val nonNull = colCount.filterKeys { it in sumCols }
    return PartialAgg(count, mins, maxs, sums, nonNull)
}

private fun updateColumn(
    rec: Record,
    col: String,
    mins: MutableMap<String, Comparable<*>?>,
    maxs: MutableMap<String, Comparable<*>?>,
    colSum: MutableMap<String, Double>,
    colCount: MutableMap<String, Long>,
) {
    val v = rec.getField(col) ?: return
    @Suppress("UNCHECKED_CAST")
    val c = (v as? Comparable<Any>) ?: return
    val curMin = mins[col]
    val curMax = maxs[col]
    if (curMin == null || (curMin as Comparable<Any>).compareTo(c) > 0) mins[col] = c
    if (curMax == null || (curMax as Comparable<Any>).compareTo(c) < 0) maxs[col] = c
    val d = (v as? Number)?.toDouble() ?: return
    colSum[col] = (colSum[col] ?: 0.0) + d
    colCount[col] = (colCount[col] ?: 0) + 1
}

/**
 * 把各文件的局部聚合合并成全局一个 [PartialAgg]（COUNT 求和，MIN/MAX 取跨文件最值，SUM/非空计数累加）。
 * 输出仍是 [PartialAgg]，不直接出 Row——这样 Combine 内部的 coder 就是普通的 SerializableCoder，
 * 避免 Beam 在 Combine 内部推不出 Row coder；真正拼成 Row 交给下游的 [AggregateToRowFn]。
 */
class AggregateCombineFn(
    private val specs: List<AggSpec>,
) : Combine.CombineFn<PartialAgg, PartialAgg, PartialAgg>(), java.io.Serializable {

    override fun createAccumulator(): PartialAgg = PartialAgg()

    override fun addInput(accumulator: PartialAgg, input: PartialAgg): PartialAgg {
        val mins = merge(accumulator.mins, input.mins, ::minOfSafe)
        val maxs = merge(accumulator.maxs, input.maxs, ::maxOfSafe)
        val sums = mergeDouble(accumulator.sums, input.sums)
        val nonNull = mergeLong(accumulator.nonNull, input.nonNull)
        return PartialAgg(accumulator.count + input.count, mins, maxs, sums, nonNull)
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

    private fun mergeDouble(a: Map<String, Double>, b: Map<String, Double>): Map<String, Double> {
        val keys = a.keys + b.keys
        return keys.associateWith { (a[it] ?: 0.0) + (b[it] ?: 0.0) }
    }

    private fun mergeLong(a: Map<String, Long>, b: Map<String, Long>): Map<String, Long> {
        val keys = a.keys + b.keys
        return keys.associateWith { (a[it] ?: 0) + (b[it] ?: 0) }
    }

    private fun minOfSafe(a: Comparable<*>, b: Comparable<*>): Comparable<*> =
        if ((a as Comparable<Any>).compareTo(b as Any) <= 0) a else b

    private fun maxOfSafe(a: Comparable<*>, b: Comparable<*>): Comparable<*> =
        if ((a as Comparable<Any>).compareTo(b as Any) >= 0) a else b
}

/**
 * 把合并后的 [PartialAgg] 拼成最终的一行 Row。单列在 [aggregateSchema] 里已定好类型，
 * 这里只按 specs 顺序取 count/min/max/sum/avg 的值填进 Row；AVG 用 `sums/nonNull` 还原，无非空值则为 null。
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
                "sum" -> builder.addValue(element.sums[col])
                "avg" -> {
                    val c = element.nonNull[col] ?: 0L
                    builder.addValue(if (c == 0L) null else (element.sums[col] ?: 0.0) / c)
                }
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
