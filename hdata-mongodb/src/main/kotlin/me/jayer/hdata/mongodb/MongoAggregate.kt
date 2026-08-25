package me.jayer.hdata.mongodb

import org.apache.beam.sdk.coders.SerializableCoder
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.Combine
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.bson.BsonDocument
import org.bson.Document
import java.io.Serializable

/**
 * MongoDB 聚合下推：把 `count` / `sum` / `min` / `max` / `avg` 翻译成 MongoDB 聚合管道。
 *
 * 与 JDBC/ES 不同，MongoDB 读取是按 `_id` 分片并行的，所以聚合走**两阶段**：
 * 1. 每个分片跑一次 `$group`（局部聚合），产出 [PartialAgg]（count 求和基数、sum 累加、min/max 最值、
 *    avg 的 sum 与非空计数）；
 * 2. 所有分片的 [PartialAgg] 在 Beam 侧 [MongoAggregateCombineFn] 全局归并，再由
 *    [MongoAggregateToRowFn] 拼成最终一行（avg = sum / 非空计数）。
 *
 * 这样聚合既真下推到 MongoDB（每个分片只在库内算），又能跨分片得到正确全局结果——等价于 Trino 的
 * "分片级局部聚合 + 最终合并"，而不是像以前那样强制单分片。
 *
 * @author wuya
 */
data class PartialAgg(
    val count: Long = 0,
    /** 按 alias 累加的 sum（sum/avg 共用）。 */
    val sums: Map<String, Double> = emptyMap(),
    /** 按 alias 取的最值（min/max）。 */
    val mins: Map<String, Double?> = emptyMap(),
    val maxs: Map<String, Double?> = emptyMap(),
    /** 按 alias 统计的非空计数（供 avg 还原）。 */
    val nonNull: Map<String, Long> = emptyMap(),
) : Serializable

/**
 * 把聚合规格翻译成 MongoDB 局部聚合管道 `[$match filter, $group {...}]`。
 * 每个分片都用 `_id: null` 做局部 `$group`；最终全局归并由 [MongoAggregateCombineFn] 完成。
 */
fun buildPartialAggregatePipeline(filter: BsonDocument, specs: List<MongoAggregateSpec>): List<Document> {
    val group = Document("_id", null)
    specs.forEach { spec ->
        when (spec.type) {
            "count" -> group.append(spec.alias, Document("\$sum", 1))
            "sum" -> group.append(spec.alias, Document("\$sum", "\$${spec.column}"))
            "avg" -> {
                // avg 拆成 sum + 非空计数两个局部累加器（key 带前缀避免与 sum 的别名冲突）
                group.append("sum_${spec.alias}", Document("\$sum", "\$${spec.column}"))
                group.append(
                    "cnt_${spec.alias}",
                    Document("\$sum", Document("\$cond", listOf(Document("\$ne", listOf("\$${spec.column}", null)), 1, 0))),
                )
            }
            "min" -> group.append(spec.alias, Document("\$min", "\$${spec.column}"))
            "max" -> group.append(spec.alias, Document("\$max", "\$${spec.column}"))
        }
    }
    return listOf(Document("\$match", filter), Document("\$group", group))
}

/** 把单个分片的 `$group` 结果文档解析成 [PartialAgg]；空分片（无文档）返回全零的 [PartialAgg]。 */
fun partialAggFromDoc(doc: Document?, specs: List<MongoAggregateSpec>): PartialAgg {
    if (doc == null) return PartialAgg()
    val sums = mutableMapOf<String, Double>()
    val mins = mutableMapOf<String, Double?>()
    val maxs = mutableMapOf<String, Double?>()
    val nonNull = mutableMapOf<String, Long>()
    val count = specs.firstOrNull { it.type == "count" }?.let { (doc[it.alias] as? Number)?.toLong() } ?: 0L
    specs.forEach { spec ->
        when (spec.type) {
            "count" -> {}
            "sum" -> sums[spec.alias] = (doc[spec.alias] as? Number)?.toDouble() ?: 0.0
            "avg" -> {
                sums[spec.alias] = (doc["sum_${spec.alias}"] as? Number)?.toDouble() ?: 0.0
                nonNull[spec.alias] = (doc["cnt_${spec.alias}"] as? Number)?.toLong() ?: 0L
            }
            "min" -> mins[spec.alias] = (doc[spec.alias] as? Number)?.toDouble()
            "max" -> maxs[spec.alias] = (doc[spec.alias] as? Number)?.toDouble()
        }
    }
    return PartialAgg(count, sums, mins, maxs, nonNull)
}

/**
 * 跨分片全局归并：count 求和，sum 累加，min/max 取跨分片最值，avg 的非空计数累加。
 * 输出仍是 [PartialAgg]，再由 [MongoAggregateToRowFn] 拼成最终 Row。
 */
class MongoAggregateCombineFn(
    private val specs: List<MongoAggregateSpec>,
) : Combine.CombineFn<PartialAgg, PartialAgg, PartialAgg>(), Serializable {

    override fun createAccumulator(): PartialAgg = PartialAgg()

    override fun addInput(accumulator: PartialAgg, input: PartialAgg): PartialAgg = merge(accumulator, input)

    override fun mergeAccumulators(accumulators: MutableIterable<PartialAgg>): PartialAgg =
        accumulators.fold(PartialAgg()) { a, b -> merge(a, b) }

    override fun extractOutput(accumulator: PartialAgg): PartialAgg = accumulator

    private fun merge(a: PartialAgg, b: PartialAgg): PartialAgg {
        val sums = (a.sums.keys + b.sums.keys).associateWith { (a.sums[it] ?: 0.0) + (b.sums[it] ?: 0.0) }
        val mins = (a.mins.keys + b.mins.keys).associateWith { pick(a.mins[it], b.mins[it], ::minOfSafe) }
        val maxs = (a.maxs.keys + b.maxs.keys).associateWith { pick(a.maxs[it], b.maxs[it], ::maxOfSafe) }
        val nonNull = (a.nonNull.keys + b.nonNull.keys).associateWith { (a.nonNull[it] ?: 0) + (b.nonNull[it] ?: 0) }
        return PartialAgg(a.count + b.count, sums, mins, maxs, nonNull)
    }

    private fun pick(a: Double?, b: Double?, f: (Double, Double) -> Double): Double? = when {
        a == null -> b
        b == null -> a
        else -> f(a, b)
    }

    private fun minOfSafe(a: Double, b: Double): Double = if (a <= b) a else b
    private fun maxOfSafe(a: Double, b: Double): Double = if (a >= b) a else b
}

/**
 * 把归并后的 [PartialAgg] 拼成最终的一行 Row。avg 用 `sums/nonNull` 还原（无非空值则为 null）。
 */
class MongoAggregateToRowFn(
    private val specs: List<MongoAggregateSpec>,
    private val outputSchema: Schema,
) : DoFn<PartialAgg, Row>() {

    @ProcessElement
    fun processElement(@Element element: PartialAgg, receiver: OutputReceiver<Row>) {
        val builder = Row.withSchema(outputSchema)
        specs.forEach { spec ->
            when (spec.type) {
                "count" -> builder.addValue(element.count)
                "sum" -> builder.addValue(element.sums[spec.alias])
                "avg" -> {
                    val c = element.nonNull[spec.alias] ?: 0L
                    builder.addValue(if (c == 0L) null else (element.sums[spec.alias] ?: 0.0) / c)
                }
                "min" -> builder.addValue(element.mins[spec.alias])
                "max" -> builder.addValue(element.maxs[spec.alias])
            }
        }
        receiver.output(builder.build())
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
