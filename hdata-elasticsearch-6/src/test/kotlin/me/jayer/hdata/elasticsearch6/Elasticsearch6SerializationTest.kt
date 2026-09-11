package me.jayer.hdata.elasticsearch6

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.util.SerializableUtils
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertNotNull

/**
 * The serialization boundary of the ES 6.x write DoFn and the source / sink produced by the provider.
 *
 * @author wuya
 */
class Elasticsearch6SerializationTest {

    private fun cfg(json: String) = TransformConfig("x", SpecMappers.CONFIG.readTree(json) as ObjectNode)

    @Test
    fun `the write DoFn ships serializable`() {
        val schema = Schema.builder().addNullableStringField("document").build()
        val fn = Elasticsearch6WriteFn(
            listOf("http://localhost:9200"), "orders", "", "", emptyList(), 1000, schema, schema, false, "WriteToElasticsearch6"
        )
        SerializableUtils.ensureSerializable(fn)
    }

    @Test
    fun `the source built by the read provider ships serializable`() {
        val transform = ReadFromElasticsearch6().from(
            cfg("""{"connection_uri":"http://localhost:9200","index":"orders","scan_slices":4}""")
        )
        assertNotNull(transform)
        SerializableUtils.ensureSerializable(transform)
    }

    @Test
    fun `the aggregation-pushdown DoFn ships serializable`() {
        SerializableUtils.ensureSerializable(
            Elasticsearch6AggregateFn(
                listOf("http://localhost:9200"), "", "", listOf("count", "min:age"), ""
            )
        )
    }

    @Test
    fun `the sink built by the write provider ships serializable`() {
        val transform = WriteToElasticsearch6().from(
            cfg("""{"connection_uri":"http://localhost:9200","index":"orders"}""")
        )
        assertNotNull(transform)
        SerializableUtils.ensureSerializable(transform)
    }
}
