package me.jayer.hdata.elasticsearch6

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode

/**
 * Verifies [Elasticsearch6WriteConfig]'s snake_case binding and [Elasticsearch6WriteConfig.validate].
 *
 * No connection to a real ES cluster: this only checks config parsing and constraints.
 */
class Elasticsearch6WriteConfigTest {

    private fun transformConfig(json: String): TransformConfig =
        TransformConfig("WriteToElasticsearch6", SpecMappers.CONFIG.readTree(json) as ObjectNode)

    @Test
    fun `the write config binds snake_case and the provider produces a transform`() {
        val cfg = transformConfig(
            """
            {
              "connection_uri": "http://localhost:9200",
              "index": "orders",
              "schema_fields": ["id:INT64", "name:STRING"],
              "batch_size": 1000
            }
            """.trimIndent()
        )

        val transform = WriteToElasticsearch6().from(cfg)
        assertNotNull(transform)
    }

    @Test
    fun `write-side default values bind correctly`() {
        val cfg = transformConfig(
            """
            {
              "connection_uri": "http://localhost:9200",
              "index": "orders"
            }
            """.trimIndent()
        )

        val config = cfg.bind(Elasticsearch6WriteConfig::class.java)
        assertEquals(emptyList<String>(), config.schemaFields)
        assertEquals(1000, config.batchSize)

        val transform = WriteToElasticsearch6().from(cfg)
        assertNotNull(transform)
    }

    @Test
    fun `validate fails when the write side's connection_uri is empty`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            Elasticsearch6WriteConfig(index = "orders").validate()
        }
        assertNotNull(error.message)
    }

    @Test
    fun `the write side's connection_uri must contain a valid HTTP node`() {
        assertThrows(IllegalArgumentException::class.java) {
            Elasticsearch6WriteConfig(connectionUri = "http://localhost:9200,", index = "orders").validate()
        }
    }

    @Test
    fun `validate fails when the write side's index is empty`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            Elasticsearch6WriteConfig(connectionUri = "http://localhost:9200").validate()
        }
        assertNotNull(error.message)
    }

    @Test
    fun `validate fails when the write side's batch_size is not positive`() {
        assertThrows(IllegalArgumentException::class.java) {
            Elasticsearch6WriteConfig(
                connectionUri = "http://localhost:9200",
                index = "orders",
                batchSize = 0,
            ).validate()
        }
        assertThrows(IllegalArgumentException::class.java) {
            Elasticsearch6WriteConfig(
                connectionUri = "http://localhost:9200",
                index = "orders",
                batchSize = -5,
            ).validate()
        }
    }

    @Test
    fun `a valid write config does not throw on validate`() {
        Elasticsearch6WriteConfig(
            connectionUri = "http://localhost:9200,http://node2:9200",
            index = "orders",
            batchSize = 200,
        ).validate()
    }

    @Test
    fun `duplicate schema fields are rejected`() {
        assertThrows(IllegalArgumentException::class.java) {
            Elasticsearch6WriteConfig(
                connectionUri = "http://localhost:9200",
                index = "orders",
                schemaFields = listOf("id:INT64", "id:STRING"),
            ).validate()
        }
    }
}
