package me.jayer.hdata.mongodb

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.mongodb.transform.MongoPartialAggregateFn
import me.jayer.hdata.mongodb.transform.MongoReadFn
import me.jayer.hdata.mongodb.transform.MongoReadSplit
import org.apache.beam.sdk.util.SerializableUtils
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test

/**
 * Various serializable boundaries on the read side: the Splittable DoFn, the partition description, and the source
 * produced by the provider's graph construction.
 *
 * These objects capture the connection string / codec / partition filter conditions, and must be serializable and
 * shippable before the job is submitted; once a non-serializable thing slips inside, unit tests that only call
 * `processElement` will never catch it — it only blows up when the whole job is submitted.
 *
 * @author wuya
 */
class MongoSerializationTest {

    @Test
    fun `the read DoFn can be serialized and shipped`() {
        val fn = MongoReadFn("mongodb://localhost:27017", MongoRowCodec.of(emptyList()), 1000)
        SerializableUtils.ensureSerializable(fn)
    }

    @Test
    fun `the partial aggregation DoFn can be serialized and shipped`() {
        val specs = listOf(MongoAggregateSpec("count", "", "total"), MongoAggregateSpec("avg", "amount", "avg_amount"))
        SerializableUtils.ensureSerializable(MongoPartialAggregateFn("mongodb://localhost:27017", specs))
    }

    @Test
    fun `the split description can be serialized and shipped`() {
        val split = MongoReadSplit("mydb", "orders", listOf("""{"_id": 1}"""))
        SerializableUtils.ensureSerializable(split)
    }

    @Test
    fun `the source produced by the read provider can be serialized and shipped`() {
        val transform = MongoReadProvider().from(
            TransformConfig(
                "ReadFromMongoDb",
                SpecMappers.CONFIG.readTree(
                    """{"connection_uri": "mongodb://localhost:27017", "database": "d", "collection": "c"}"""
                ) as ObjectNode,
            )
        )
        SerializableUtils.ensureSerializable(transform)
    }
}
