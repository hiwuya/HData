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
 * The serialization boundary of the ES 8.x write DoFn and the sink produced by the provider.
 *
 * Non-serializable client objects such as `Query` / `SortOptions` on the read side once ended up as
 * plain DoFn fields, so the job only blew up at submission time; they now all live in `@Setup`. The
 * write side's `EsWriteFn` likewise must serialize along with the job — a test that only calls
 * `processElement` would never catch a problem like that.
 *
 * @author wuya
 */
class EsSerializationTest {

    private fun cfg(json: String) = TransformConfig("x", SpecMappers.CONFIG.readTree(json) as ObjectNode)

    @Test
    fun `the write DoFn is serializable for submission and carries no non-serializable client object`() {
        val schema = Schema.builder().addNullableStringField("document").build()
        val config = EsWriteConfig(connectionUri = "http://localhost:9200", index = "orders")
        SerializableUtils.ensureSerializable(EsWriteFn(config, schema, schema, false, "WriteToElasticsearch8"))
    }

    @Test
    fun `the sink produced by the write provider is serializable for submission`() {
        val transform = EsWriteProvider().from(
            cfg("""{"connection_uri":"http://localhost:9200","index":"orders"}""")
        )
        assertNotNull(transform)
        SerializableUtils.ensureSerializable(transform)
    }

    @Test
    fun `the aggregation pushdown DoFn is serializable for submission`() {
        val config = EsReadConfig(connectionUri = "http://localhost:9200", index = "orders", aggregations = listOf("count", "min:age"))
        SerializableUtils.ensureSerializable(EsAggregateFn(config))
    }
}
