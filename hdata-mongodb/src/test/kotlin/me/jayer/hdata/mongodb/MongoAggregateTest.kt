package me.jayer.hdata.mongodb

import me.jayer.hdata.mongodb.transform.buildAggregatePipeline
import org.apache.beam.sdk.schemas.Schema
import org.bson.BsonDocument
import org.bson.Document
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 聚合下推的纯逻辑：管道结构（`buildAggregatePipeline`）与输出 schema（`aggregateSchema`）。
 * 这两块决定了"聚合到底有没有真下推、下成了什么"，必须单测守住。
 */
class MongoAggregateTest {

    @Test
    fun `聚合管道是 match 加 group，count 用 sum 1 其余用对应累加器`() {
        val filter = BsonDocument.parse("""{"status": "PAID"}""")
        val specs = listOf(
            MongoAggregateSpec("count", "", "total"),
            MongoAggregateSpec("min", "amount", "min_amount"),
            MongoAggregateSpec("max", "amount", "max_amount"),
            MongoAggregateSpec("sum", "amount", "sum_amount"),
            MongoAggregateSpec("avg", "amount", "avg_amount"),
        )

        val pipeline = buildAggregatePipeline(filter, specs)

        assertEquals(2, pipeline.size)
        assertEquals(filter, pipeline[0]["\$match"])
        val group = pipeline[1]["\$group"] as Document
        assertEquals(null, group["_id"])
        assertEquals(Document("\$sum", 1), group["total"])
        assertEquals(Document("\$min", "\$amount"), group["min_amount"])
        assertEquals(Document("\$max", "\$amount"), group["max_amount"])
        assertEquals(Document("\$sum", "\$amount"), group["sum_amount"])
        assertEquals(Document("\$avg", "\$amount"), group["avg_amount"])
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
}
