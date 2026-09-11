package me.jayer.hdata.elasticsearch6

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.apache.beam.sdk.schemas.Schema
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode

/**
 * Verifies [Elasticsearch6ReadConfig]'s snake_case binding and [Elasticsearch6ReadConfig.validate].
 *
 * No connection to a real ES cluster: this only checks config parsing and constraints.
 */
class Elasticsearch6ReadConfigTest {

    private fun transformConfig(json: String): TransformConfig =
        TransformConfig("ReadFromElasticsearch6", SpecMappers.CONFIG.readTree(json) as ObjectNode)

    @Test
    fun `read config binds snake_case and the provider produces a transform`() {
        val cfg = transformConfig(
            """
            {
              "connection_uri": "http://localhost:9200",
              "index": "orders",
              "schema_fields": ["id:INT64", "name:STRING", "amount:DOUBLE"],
              "scroll_size": 1000,
              "scroll_timeout_minutes": 1
            }
            """.trimIndent()
        )

        val transform = ReadFromElasticsearch6().from(cfg)
        assertNotNull(transform)
    }

    @Test
    fun `multiple read indices and defaults bind correctly`() {
        val cfg = transformConfig(
            """
            {
              "connection_uri": "http://localhost:9200,http://node2:9200",
              "indices": ["a", "b"],
              "scroll_size": 500
            }
            """.trimIndent()
        )

        val config = cfg.bind(Elasticsearch6ReadConfig::class.java)
        assertEquals(listOf("http://localhost:9200", "http://node2:9200"), config.nodes())
        assertEquals(listOf("a", "b"), config.indexList())
        assertEquals(500, config.scrollSize)
        assertEquals(1, config.scrollTimeoutMinutes)
        assertEquals(emptyList<String>(), config.schemaFields)

        val transform = ReadFromElasticsearch6().from(cfg)
        assertNotNull(transform)
    }

    @Test
    fun `validate fails when connection_uri is empty`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            Elasticsearch6ReadConfig(index = "orders").validate()
        }
        assertNotNull(error.message)
    }

    @Test
    fun `connection_uri must contain a valid HTTP node`() {
        listOf("localhost:9200", "ftp://localhost:9200").forEach { uri ->
            assertThrows(IllegalArgumentException::class.java) {
                Elasticsearch6ReadConfig(connectionUri = uri, index = "orders").validate()
            }
        }
    }

    @Test
    fun `password without username is rejected instead of connecting anonymously`() {
        assertThrows(IllegalArgumentException::class.java) {
            Elasticsearch6ReadConfig(
                connectionUri = "http://localhost:9200",
                index = "orders",
                password = "secret",
            ).validate()
        }
        Elasticsearch6ReadConfig(
            connectionUri = "http://localhost:9200",
            index = "orders",
            username = "elastic",
        ).validate()
    }

    @Test
    fun `validate fails when both index and indices are missing`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            Elasticsearch6ReadConfig(connectionUri = "http://localhost:9200").validate()
        }
        assertNotNull(error.message)
    }

    @Test
    fun `validate fails when scroll_size is not positive`() {
        assertThrows(IllegalArgumentException::class.java) {
            Elasticsearch6ReadConfig(
                connectionUri = "http://localhost:9200",
                index = "orders",
                scrollSize = 0,
            ).validate()
        }
        assertThrows(IllegalArgumentException::class.java) {
            Elasticsearch6ReadConfig(
                connectionUri = "http://localhost:9200",
                index = "orders",
                scrollSize = -1,
            ).validate()
        }
    }

    @Test
    fun `a valid read config does not throw on validate`() {
        Elasticsearch6ReadConfig(
            connectionUri = "http://localhost:9200",
            index = "orders",
            scrollSize = 100,
        ).validate()
    }

    @Test
    fun `index and indices are mutually exclusive, and the list rejects blanks`() {
        assertThrows(IllegalArgumentException::class.java) {
            Elasticsearch6ReadConfig(
                connectionUri = "http://localhost:9200",
                index = "orders",
                indices = listOf("archive"),
            ).validate()
        }
        assertThrows(IllegalArgumentException::class.java) {
            Elasticsearch6ReadConfig(
                connectionUri = "http://localhost:9200",
                indices = listOf("a", " "),
            ).validate()
        }
        assertThrows(IllegalArgumentException::class.java) {
            Elasticsearch6ReadConfig(
                connectionUri = "http://localhost:9200",
                indices = listOf("a", "a"),
            ).validate()
        }
    }

    @Test
    fun `aggregation pushdown config validation`() {
        val base = Elasticsearch6ReadConfig(connectionUri = "http://localhost:9200", index = "orders")
        base.copy(aggregations = listOf("count", "min:age", "max:age", "sum:age", "avg:age")).validate()
        assertThrows(IllegalArgumentException::class.java) {
            base.copy(aggregations = listOf("count"), schemaFields = listOf("age:DOUBLE")).validate()
        }
        assertThrows(IllegalArgumentException::class.java) { base.copy(aggregations = listOf("count"), limit = 5).validate() }
        assertThrows(IllegalArgumentException::class.java) { base.copy(aggregations = listOf("count"), scanSlices = 2).validate() }
        assertThrows(IllegalArgumentException::class.java) { base.copy(aggregations = listOf("count"), scrollSize = 10).validate() }
        assertThrows(IllegalArgumentException::class.java) {
            base.copy(aggregations = listOf("count"), scrollTimeoutMinutes = 2).validate()
        }
        assertThrows(IllegalArgumentException::class.java) { base.copy(aggregations = listOf("median")).validate() }
        assertThrows(IllegalArgumentException::class.java) { base.copy(aggregations = listOf("count:age")).validate() }
        assertThrows(IllegalArgumentException::class.java) { base.copy(aggregations = listOf("min:age", "min:age")).validate() }
    }

    @Test
    fun `limit rejects multiple indices and an ignored scan_slices`() {
        val base = Elasticsearch6ReadConfig(connectionUri = "http://localhost:9200", index = "orders")
        assertThrows(IllegalArgumentException::class.java) { base.copy(limit = 5, scanSlices = 2).validate() }
        assertThrows(IllegalArgumentException::class.java) {
            base.copy(index = "", indices = listOf("a", "b"), limit = 5).validate()
        }
    }

    @Test
    fun `aggregation result schema field names and types`() {
        val schema = buildAggregateSchema(parseEs6Aggregations(listOf("count", "min:age", "max:age", "sum:age", "avg:age")))
        assertEquals(listOf("count", "min_age", "max_age", "sum_age", "avg_age"), schema.fieldNames)
        assertEquals(Schema.TypeName.INT64, schema.getField("count").type.typeName)
        assertEquals(Schema.TypeName.DOUBLE, schema.getField("min_age").type.typeName)
    }
}
