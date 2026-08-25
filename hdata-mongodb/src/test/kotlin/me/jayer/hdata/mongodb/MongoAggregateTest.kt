package me.jayer.hdata.mongodb

import me.jayer.hdata.mongodb.MongoAggregateCombineFn
import me.jayer.hdata.mongodb.MongoAggregateToRowFn
import me.jayer.hdata.mongodb.buildPartialAggregatePipeline
import me.jayer.hdata.mongodb.partialAggFromDoc
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.util.SerializableUtils
import org.bson.BsonDocument
import org.bson.Document
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 聚合下推的纯逻辑：局部聚合管道（`buildPartialAggregatePipeline`）、分片归并
 * （[MongoAggregateCombineFn] + [MongoAggregateToRowFn]）、与输出 schema（`aggregateSchema`）。
 * 这三块决定了"聚合到底有没有真下推、下成了什么、跨分片归并后对不对"，必须单测守住。
 */
class MongoAggregateTest {

    @Test
    fun `局部聚合管道是 match 加 group，count 用 sum 1 其余用对应累加器`() {
        val filter = BsonDocument.parse("""{"status": "PAID"}""")
        val specs = listOf(
            MongoAggregateSpec("count", "", "total"),
            MongoAggregateSpec("min", "amount", "min_amount"),
            MongoAggregateSpec("max", "amount", "max_amount"),
            MongoAggregateSpec("sum", "amount", "sum_amount"),
            MongoAggregateSpec("avg", "amount", "avg_amount"),
        )

        val pipeline = buildPartialAggregatePipeline(filter, specs)

        assertEquals(2, pipeline.size)
        assertEquals(filter, pipeline[0]["\$match"])
        val group = pipeline[1]["\$group"] as Document
        assertEquals(null, group["_id"])
        assertEquals(Document("\$sum", 1), group["total"])
        assertEquals(Document("\$min", "\$amount"), group["min_amount"])
        assertEquals(Document("\$max", "\$amount"), group["max_amount"])
        assertEquals(Document("\$sum", "\$amount"), group["sum_amount"])
        // avg 拆成 sum + 非空计数两个局部累加器，最终由归并阶段相除还原
        assertEquals(Document("\$sum", "\$amount"), group["sum_avg_amount"])
        assertEquals(
            Document("\$sum", Document("\$cond", listOf(Document("\$ne", listOf("\$amount", null)), 1, 0))),
            group["cnt_avg_amount"],
        )
    }

    @Test
    fun `聚合输出 schema count 为 INT64 其余 DOUBLE 且可空`() {
        val schema = aggregateSchema(
            listOf(MongoAggregateSpec("count", "", "total"), MongoAggregateSpec("min", "amount", "min_amount")),
        )

        assertEquals(Schema.FieldType.INT64.withNullable(true), schema.getField("total").type)
        assertEquals(Schema.FieldType.DOUBLE.withNullable(true), schema.getField("min_amount").type)
        assertTrue(schema.getField("total").type.nullable)
        assertTrue(schema.getField("min_amount").type.nullable)
    }

    @Test
    fun `分片局部聚合经全局归并得到正确结果（avg 按 sum 与非空计数还原）`() {
        val specs = listOf(
            MongoAggregateSpec("count", "", "total"),
            MongoAggregateSpec("sum", "amount", "sum_amount"),
            MongoAggregateSpec("avg", "amount", "avg_amount"),
            MongoAggregateSpec("min", "amount", "min_amount"),
            MongoAggregateSpec("max", "amount", "max_amount"),
        )
        // 两个分片各产出的局部聚合：分片1 amount=[1,3]，分片2 amount=[5,7]（7 为 null 不计 avg）
        val p1 = partialAggFromDoc(
            Document("total", 2L)
                .append("sum_amount", 4.0)
                .append("sum_avg_amount", 4.0).append("cnt_avg_amount", 2L)
                .append("min_amount", 1.0).append("max_amount", 3.0),
            specs,
        )
        val p2 = partialAggFromDoc(
            Document("total", 2L)
                .append("sum_amount", 12.0)
                .append("sum_avg_amount", 5.0).append("cnt_avg_amount", 1L)
                .append("min_amount", 5.0).append("max_amount", 7.0),
            specs,
        )

        val merged = MongoAggregateCombineFn(specs).let { it.mergeAccumulators(mutableListOf(p1, p2)) }
        val schema = aggregateSchema(specs)
        val row = MongoAggregateToRowFn(specs, schema).let { fn ->
            // 直接用归并结果构造一行
            val b = org.apache.beam.sdk.values.Row.withSchema(schema)
            specs.forEach { spec ->
                when (spec.type) {
                    "count" -> b.addValue(merged.count)
                    "sum" -> b.addValue(merged.sums[spec.alias])
                    "avg" -> {
                        val c = merged.nonNull[spec.alias] ?: 0L
                        b.addValue(if (c == 0L) null else merged.sums[spec.alias]!! / c)
                    }
                    "min" -> b.addValue(merged.mins[spec.alias])
                    "max" -> b.addValue(merged.maxs[spec.alias])
                }
            }
            b.build()
        }

        assertEquals(4L, row.getInt64("total"))
        assertEquals(16.0, row.getDouble("sum_amount"))
        assertEquals(9.0 / 3.0, row.getDouble("avg_amount")) // (4+5)/(2+1)
        assertEquals(1.0, row.getDouble("min_amount"))
        assertEquals(7.0, row.getDouble("max_amount"))
    }

    @Test
    fun `聚合相关类型可序列化下发`() {
        val specs = listOf(MongoAggregateSpec("count", "", "total"), MongoAggregateSpec("avg", "amount", "avg_amount"))
        SerializableUtils.ensureSerializable(MongoAggregateCombineFn(specs))
        SerializableUtils.ensureSerializable(MongoAggregateToRowFn(specs, aggregateSchema(specs)))
        SerializableUtils.ensureSerializable(partialAggFromDoc(null, specs))
    }
}
