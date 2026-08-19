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
 * 验证 [EsReadConfig] 的 snake_case 绑定与 [EsReadConfig.validate]。
 */
class EsReadConfigTest {

    private fun transformConfig(json: String): TransformConfig =
        TransformConfig("ReadFromElasticsearch8", SpecMappers.CONFIG.readTree(json) as ObjectNode)

    @Test
    fun `读端配置按 snake_case 绑定并经 provider 生成 transform`() {
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
    fun `读端 indices 列表可被绑定`() {
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
    fun `读端未知配置键会报错`() {
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
    fun `读端 connection_uri 为空时报错`() {
        assertThrows(IllegalArgumentException::class.java) {
            EsReadConfig(index = "orders").validate()
        }
    }

    @Test
    fun `读端 connection_uri 必须包含合法 HTTP 节点`() {
        listOf("localhost:9200", "ftp://localhost:9200", "http://localhost:9200,").forEach { uri ->
            assertThrows(IllegalArgumentException::class.java) {
                EsReadConfig(connectionUri = uri, index = "orders").validate()
            }
        }
    }

    @Test
    fun `读端 index 和 indices 都为空时报错`() {
        assertThrows(IllegalArgumentException::class.java) {
            EsReadConfig(connectionUri = "http://localhost:9200").validate()
        }
    }

    @Test
    fun `读端 index 与 indices 互斥且列表不收空值`() {
        assertThrows(IllegalArgumentException::class.java) {
            EsReadConfig(connectionUri = "http://localhost:9200", index = "a", indices = listOf("b")).validate()
        }
        assertThrows(IllegalArgumentException::class.java) {
            EsReadConfig(connectionUri = "http://localhost:9200", indices = listOf("a", " ")).validate()
        }
    }

    @Test
    fun `读端 batch_size 非正时报错`() {
        assertThrows(IllegalArgumentException::class.java) {
            EsReadConfig(connectionUri = "http://localhost:9200", index = "orders", batchSize = 0).validate()
        }
        assertThrows(IllegalArgumentException::class.java) {
            EsReadConfig(connectionUri = "http://localhost:9200", index = "orders", batchSize = -1).validate()
        }
    }

    @Test
    fun `合法读端配置 validate 不抛异常`() {
        EsReadConfig(connectionUri = "http://localhost:9200", index = "orders").validate()
        EsReadConfig(connectionUri = "http://localhost:9200", indices = listOf("a", "b")).validate()
    }

    companion object {
        @Suppress("unused")
        private val SCHEMA: Schema = Schema.builder()
            .addNullableField("document", Schema.FieldType.STRING)
            .build()
    }
}
