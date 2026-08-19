package me.jayer.hdata.elasticsearch6

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.util.SerializableUtils
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertNotNull

/**
 * ES 6.x 写入 DoFn 与 provider 生成的 source / sink 的序列化边界。
 *
 * @author wuya
 */
class Elasticsearch6SerializationTest {

    private fun cfg(json: String) = TransformConfig("x", SpecMappers.CONFIG.readTree(json) as ObjectNode)

    @Test
    fun `写入 DoFn 可序列化下发`() {
        val schema = Schema.builder().addNullableStringField("document").build()
        val fn = Elasticsearch6WriteFn(
            listOf("http://localhost:9200"), "orders", "", "", emptyList(), 1000, schema, schema, false, "WriteToElasticsearch6"
        )
        SerializableUtils.ensureSerializable(fn)
    }

    @Test
    fun `读取 provider 生成的 source 可序列化下发`() {
        val transform = ReadFromElasticsearch6().from(
            cfg("""{"connection_uri":"http://localhost:9200","index":"orders","scan_slices":4}""")
        )
        assertNotNull(transform)
        SerializableUtils.ensureSerializable(transform)
    }

    @Test
    fun `写入 provider 生成的 sink 可序列化下发`() {
        val transform = WriteToElasticsearch6().from(
            cfg("""{"connection_uri":"http://localhost:9200","index":"orders"}""")
        )
        assertNotNull(transform)
        SerializableUtils.ensureSerializable(transform)
    }
}
