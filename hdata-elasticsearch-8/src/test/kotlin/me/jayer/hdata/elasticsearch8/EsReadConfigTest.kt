package me.jayer.hdata.elasticsearch8

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.apache.beam.sdk.schemas.Schema
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode

/**
 * Verifies [EsReadConfig]'s snake_case binding and [EsReadConfig.validate].
 */
class EsReadConfigTest {

    private fun transformConfig(json: String): TransformConfig =
        TransformConfig("ReadFromElasticsearch8", SpecMappers.CONFIG.readTree(json) as ObjectNode)

    @Test
    fun `read config binds via snake_case and the provider produces a transform`() {
        val cfg = transformConfig(
            """
            {
              "connection_uri": "http://localhost:9200",
              "index": "orders",
              "schema_fields": ["id:STRING", "amount:DOUBLE"],
              "batch_size": 500
            }
            """.trimIndent()
        )

        val transform = EsReadProvider().from(cfg)
        assertNotNull(transform)
    }

    @Test
    fun `the read-side indices list binds correctly`() {
        val cfg = transformConfig(
            """
            {
              "connection_uri": "http://localhost:9200",
              "indices": ["orders", "customers"],
              "batch_size": 200
            }
            """.trimIndent()
        )
        val config = cfg.bind(EsReadConfig::class.java)

        assertEquals("http://localhost:9200", config.connectionUri)
        assertEquals(listOf("orders", "customers"), config.indices)
        assertEquals(200, config.batchSize)
    }

    @Test
    fun `an unknown read-side config key errors`() {
        val cfg = transformConfig(
            """
            {
              "connection_uri": "http://localhost:9200",
              "index": "orders",
              "typo_key": 123
            }
            """.trimIndent()
        )
        assertThrows(HDataException::class.java) {
            EsReadProvider().from(cfg)
        }
    }

    @Test
    fun `an empty read-side connection_uri errors`() {
        assertThrows(IllegalArgumentException::class.java) {
            EsReadConfig(index = "orders").validate()
        }
    }

    @Test
    fun `read-side connection_uri must contain a valid HTTP node`() {
        listOf("localhost:9200", "ftp://localhost:9200", "http://localhost:9200,").forEach { uri ->
            assertThrows(IllegalArgumentException::class.java) {
                EsReadConfig(connectionUri = uri, index = "orders").validate()
            }
        }
    }

    @Test
    fun `auth methods are mutually exclusive, and password cannot be silently ignored`() {
        val base = EsReadConfig(connectionUri = "http://localhost:9200", index = "orders")
        assertThrows(IllegalArgumentException::class.java) { base.copy(password = "secret").validate() }
        assertThrows(IllegalArgumentException::class.java) {
            base.copy(apiKey = "token", username = "elastic", password = "secret").validate()
        }
        base.copy(username = "elastic", password = "").validate()
    }

    @Test
    fun `an error is raised when both index and indices are empty`() {
        assertThrows(IllegalArgumentException::class.java) {
            EsReadConfig(connectionUri = "http://localhost:9200").validate()
        }
    }

    @Test
    fun `index and indices are mutually exclusive, and the list rejects blank entries`() {
        assertThrows(IllegalArgumentException::class.java) {
            EsReadConfig(connectionUri = "http://localhost:9200", index = "a", indices = listOf("b")).validate()
        }
        assertThrows(IllegalArgumentException::class.java) {
            EsReadConfig(connectionUri = "http://localhost:9200", indices = listOf("a", " ")).validate()
        }
        assertThrows(IllegalArgumentException::class.java) {
            EsReadConfig(connectionUri = "http://localhost:9200", indices = listOf("a", "a")).validate()
        }
    }

    @Test
    fun `a non-positive read-side batch_size errors`() {
        assertThrows(IllegalArgumentException::class.java) {
            EsReadConfig(connectionUri = "http://localhost:9200", index = "orders", batchSize = 0).validate()
        }
        assertThrows(IllegalArgumentException::class.java) {
            EsReadConfig(connectionUri = "http://localhost:9200", index = "orders", batchSize = -1).validate()
        }
    }

    @Test
    fun `validate does not throw for a valid read-side config`() {
        EsReadConfig(connectionUri = "http://localhost:9200", index = "orders").validate()
        EsReadConfig(connectionUri = "http://localhost:9200", indices = listOf("a", "b")).validate()
    }

    @Test
    fun `aggregation pushdown config validation`() {
        val base = EsReadConfig(connectionUri = "http://localhost:9200", index = "orders")
        // count/min/max/sum/avg are all supported
        base.copy(aggregations = listOf("count", "min:age", "max:age", "sum:age", "avg:age")).validate()
        // aggregation mode rejects config that would have no effect
        assertThrows(IllegalArgumentException::class.java) {
            base.copy(aggregations = listOf("count"), schemaFields = listOf("age:DOUBLE")).validate()
        }
        assertThrows(IllegalArgumentException::class.java) { base.copy(aggregations = listOf("count"), limit = 5).validate() }
        assertThrows(IllegalArgumentException::class.java) { base.copy(aggregations = listOf("count"), scanSlices = 2).validate() }
        assertThrows(IllegalArgumentException::class.java) { base.copy(aggregations = listOf("count"), batchSize = 10).validate() }
        assertThrows(IllegalArgumentException::class.java) { base.copy(aggregations = listOf("count"), keepAliveMinutes = 10).validate() }
        // an unsupported aggregation errors directly
        assertThrows(IllegalArgumentException::class.java) { base.copy(aggregations = listOf("median")).validate() }
        assertThrows(IllegalArgumentException::class.java) { base.copy(aggregations = listOf("count:age")).validate() }
        assertThrows(IllegalArgumentException::class.java) { base.copy(aggregations = listOf("min:age", "min:age")).validate() }
    }

    @Test
    fun `limit rejects multiple indices and a scan_slices that would be ignored`() {
        val base = EsReadConfig(connectionUri = "http://localhost:9200", index = "orders")
        assertThrows(IllegalArgumentException::class.java) { base.copy(limit = 5, scanSlices = 2).validate() }
        assertThrows(IllegalArgumentException::class.java) {
            base.copy(index = "", indices = listOf("a", "b"), limit = 5).validate()
        }
    }

    @Test
    fun `aggregation result schema field names and types`() {
        val schema = buildAggregateSchema(parseEsAggregations(listOf("count", "min:age", "max:age", "sum:age", "avg:age")))
        assertEquals(listOf("count", "min_age", "max_age", "sum_age", "avg_age"), schema.fieldNames)
        assertEquals(Schema.TypeName.INT64, schema.getField("count").type.typeName)
        assertEquals(Schema.TypeName.DOUBLE, schema.getField("min_age").type.typeName)
    }

    companion object {
        @Suppress("unused")
        private val SCHEMA: Schema = Schema.builder()
            .addNullableField("document", Schema.FieldType.STRING)
            .build()
    }
}
