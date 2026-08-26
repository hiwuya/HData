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
 * 读取端各类可序列化边界：Splittable DoFn、分片描述、以及 provider 构图生成的 source。
 *
 * 这些对象捕获了连接串 / codec / 分片过滤条件，必须在提交作业前就能序列化下发；
 * 一旦里面夹了不可序列化的东西，单测只调 `processElement` 永远发现不了，只有整作业提交才炸。
 *
 * @author wuya
 */
class MongoSerializationTest {

    @Test
    fun `读取 DoFn 可序列化下发`() {
        val fn = MongoReadFn("mongodb://localhost:27017", MongoRowCodec.of(emptyList()), 1000)
        SerializableUtils.ensureSerializable(fn)
    }

    @Test
    fun `局部聚合 DoFn 可序列化下发`() {
        val specs = listOf(MongoAggregateSpec("count", "", "total"), MongoAggregateSpec("avg", "amount", "avg_amount"))
        SerializableUtils.ensureSerializable(MongoPartialAggregateFn("mongodb://localhost:27017", specs))
    }

    @Test
    fun `分片描述可序列化下发`() {
        val split = MongoReadSplit("mydb", "orders", listOf("""{"_id": 1}"""))
        SerializableUtils.ensureSerializable(split)
    }

    @Test
    fun `读取 provider 生成的 source 可序列化下发`() {
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
