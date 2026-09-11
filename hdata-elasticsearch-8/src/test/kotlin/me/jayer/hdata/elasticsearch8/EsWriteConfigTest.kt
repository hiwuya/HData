package me.jayer.hdata.elasticsearch8

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode

/**
 * Verifies [EsWriteConfig]'s snake_case binding and [EsWriteConfig.validate].
 */
class EsWriteConfigTest {

    private fun transformConfig(json: String): TransformConfig =
        TransformConfig("WriteToElasticsearch8", SpecMappers.CONFIG.readTree(json) as ObjectNode)

    @Test
    fun `write config binds via snake_case and the provider produces a transform`() {
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

        val transform = EsWriteProvider().from(cfg)
        assertNotNull(transform)
    }

    @Test
    fun `an unknown write-side config key errors`() {
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
            EsWriteProvider().from(cfg)
        }
    }

    @Test
    fun `an empty write-side connection_uri errors`() {
        assertThrows(IllegalArgumentException::class.java) {
            EsWriteConfig(index = "orders").validate()
        }
    }

    @Test
    fun `write-side connection_uri must contain a valid HTTP node`() {
        assertThrows(IllegalArgumentException::class.java) {
            EsWriteConfig(connectionUri = "not-a-uri", index = "orders").validate()
        }
    }

    @Test
    fun `an empty write-side index errors`() {
        assertThrows(IllegalArgumentException::class.java) {
            EsWriteConfig(connectionUri = "http://localhost:9200").validate()
        }
    }

    @Test
    fun `a non-positive write-side batch_size errors`() {
        assertThrows(IllegalArgumentException::class.java) {
            EsWriteConfig(connectionUri = "http://localhost:9200", index = "orders", batchSize = 0).validate()
        }
    }

    @Test
    fun `validate does not throw for a valid write-side config`() {
        EsWriteConfig(connectionUri = "http://localhost:9200", index = "orders").validate()
    }

    @Test
    fun `write-side schema_fields validates type and duplicate names`() {
        assertThrows(IllegalArgumentException::class.java) {
            EsWriteConfig(
                connectionUri = "http://localhost:9200",
                index = "orders",
                schemaFields = listOf("id:UUID"),
            ).validate()
        }
        assertThrows(IllegalArgumentException::class.java) {
            EsWriteConfig(
                connectionUri = "http://localhost:9200",
                index = "orders",
                schemaFields = listOf("id:INT64", "id:STRING"),
            ).validate()
        }
    }
}
