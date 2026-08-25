package me.jayer.hdata.elasticsearch8

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.elasticsearch8.transform.EsAggregateFn
import me.jayer.hdata.elasticsearch8.transform.EsWriteFn
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.util.SerializableUtils
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertNotNull

/**
 * ES 8.x 写入 DoFn 与 provider 生成的 sink 的序列化边界。
 *
 * 读取端 `Query` / `SortOptions` 这类不可序列化的客户端对象曾混进 DoFn 字段里，导致作业在提交阶段才炸；
 * 现在它们都被挪到 `@Setup`。写端 `EsWriteFn` 同样要能跟着作业序列化下发——单测只调 `processElement`
 * 永远发现不了这类问题。
 *
 * @author wuya
 */
class EsSerializationTest {

    private fun cfg(json: String) = TransformConfig("x", SpecMappers.CONFIG.readTree(json) as ObjectNode)

    @Test
    fun `写入 DoFn 可序列化下发，且不含不可序列化的客户端对象`() {
        val schema = Schema.builder().addNullableStringField("document").build()
        val config = EsWriteConfig(connectionUri = "http://localhost:9200", index = "orders")
        SerializableUtils.ensureSerializable(EsWriteFn(config, schema, schema, false, "WriteToElasticsearch8"))
    }

    @Test
    fun `写入 provider 生成的 sink 可序列化下发`() {
        val transform = EsWriteProvider().from(
            cfg("""{"connection_uri":"http://localhost:9200","index":"orders"}""")
        )
        assertNotNull(transform)
        SerializableUtils.ensureSerializable(transform)
    }

    @Test
    fun `聚合下推 DoFn 可序列化下发`() {
        val config = EsReadConfig(connectionUri = "http://localhost:9200", index = "orders", aggregations = listOf("count", "min:age"))
        SerializableUtils.ensureSerializable(EsAggregateFn(config))
    }
}
