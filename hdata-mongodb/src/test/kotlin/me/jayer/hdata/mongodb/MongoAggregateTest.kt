package me.jayer.hdata.mongodb

import me.jayer.hdata.mongodb.MongoAggregateCombineFn
import me.jayer.hdata.mongodb.MongoAggregateToRowFn
import me.jayer.hdata.mongodb.buildPartialAggregatePipeline
import me.jayer.hdata.mongodb.partialAggFromDoc
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.util.SerializableUtils
import org.bson.BsonDocument
import org.bson.Document
import org.bson.types.Decimal128
import java.math.BigDecimal
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Pure logic of push-down aggregation: the local aggregation pipeline (`buildPartialAggregatePipeline`), the
 * cross-partition merge ([MongoAggregateCombineFn] + [MongoAggregateToRowFn]), and the output schema
 * (`aggregateSchema`). These three pieces decide "whether the aggregation is truly pushed down, what it becomes,
 * and whether the cross-partition merge is correct", so they must be guarded by unit tests.
 */
class MongoAggregateTest {

    @Test
    fun `the partial aggregation pipeline is match plus group, count uses sum 1 and the rest their own accumulators`() {
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
        // avg is split into sum + non-null count, two local accumulators, finally divided and reconstructed in the merge stage
        assertEquals(Document("\$sum", "\$amount"), group["__hdata_avg_sum_4"])
        assertEquals(
            Document("\$sum", Document("\$cond", listOf(Document("\$ne", listOf("\$amount", null)), 1, 0))),
            group["__hdata_avg_count_4"],
        )
    }

    @Test
    fun `AVG internal fields never overwrite a user alias and Decimal128 parses correctly`() {
        val specs = listOf(
            MongoAggregateSpec("sum", "amount", "__hdata_avg_sum_1"),
            MongoAggregateSpec("avg", "amount", "average"),
        )
        val group = buildPartialAggregatePipeline(BsonDocument(), specs)[1]["\$group"] as Document
        assertEquals(Document("\$sum", "\$amount"), group["__hdata_avg_sum_1"])
        // The default internal name for index=1 collides with the user alias above, so it must automatically switch to a conflict-free key.
        assertEquals(Document("\$sum", "\$amount"), group["__hdata_avg_sum_1_"])

        val partial = partialAggFromDoc(
            Document("__hdata_avg_sum_1", Decimal128(BigDecimal("12.50")))
                .append("__hdata_avg_sum_1_", Decimal128(BigDecimal("12.50")))
                .append("__hdata_avg_count_1", 2L),
            specs,
        )
        assertEquals(12.5, partial.sums["__hdata_avg_sum_1"])
        assertEquals(12.5, partial.sums["average"])
        assertEquals(2L, partial.nonNull["average"])
    }

    @Test
    fun `in the aggregate output schema count is INT64, the rest DOUBLE, all nullable`() {
        val schema = aggregateSchema(
            listOf(MongoAggregateSpec("count", "", "total"), MongoAggregateSpec("min", "amount", "min_amount")),
        )

        assertEquals(Schema.FieldType.INT64.withNullable(true), schema.getField("total").type)
        assertEquals(Schema.FieldType.DOUBLE.withNullable(true), schema.getField("min_amount").type)
        assertTrue(schema.getField("total").type.nullable)
        assertTrue(schema.getField("min_amount").type.nullable)
    }

    @Test
    fun `per-partition partial aggregates merge globally into the correct result (avg rebuilt from sum and non-null count)`() {
        val specs = listOf(
            MongoAggregateSpec("count", "", "total"),
            MongoAggregateSpec("sum", "amount", "sum_amount"),
            MongoAggregateSpec("avg", "amount", "avg_amount"),
            MongoAggregateSpec("min", "amount", "min_amount"),
            MongoAggregateSpec("max", "amount", "max_amount"),
        )
        // The local aggregation produced by two partitions: partition1 amount=[1,3], partition2 amount=[5,7] (7 is null and excluded from avg)
        val p1 = partialAggFromDoc(
            Document("total", 2L)
                .append("sum_amount", 4.0)
                .append("__hdata_avg_sum_2", 4.0).append("__hdata_avg_count_2", 2L)
                .append("min_amount", 1.0).append("max_amount", 3.0),
            specs,
        )
        val p2 = partialAggFromDoc(
            Document("total", 2L)
                .append("sum_amount", 12.0)
                .append("__hdata_avg_sum_2", 5.0).append("__hdata_avg_count_2", 1L)
                .append("min_amount", 5.0).append("max_amount", 7.0),
            specs,
        )

        val merged = MongoAggregateCombineFn(specs).let { it.mergeAccumulators(mutableListOf(p1, p2)) }
        val schema = aggregateSchema(specs)
        val row = MongoAggregateToRowFn(specs, schema).let { fn ->
            // Build a row directly from the merge result
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
    fun `aggregation-related types can be serialized and shipped`() {
        val specs = listOf(MongoAggregateSpec("count", "", "total"), MongoAggregateSpec("avg", "amount", "avg_amount"))
        SerializableUtils.ensureSerializable(MongoAggregateCombineFn(specs))
        SerializableUtils.ensureSerializable(MongoAggregateToRowFn(specs, aggregateSchema(specs)))
        SerializableUtils.ensureSerializable(partialAggFromDoc(null, specs))
    }
}
