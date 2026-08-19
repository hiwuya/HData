package me.jayer.hdata.elasticsearch6

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode

/**
 * 验证 [Elasticsearch6WriteConfig] 的 snake_case 绑定与 [Elasticsearch6WriteConfig.validate]。
 *
 * 不连接真实 ES 集群：这里只校验配置解析与约束。
 */
class Elasticsearch6WriteConfigTest {

    private fun transformConfig(json: String): TransformConfig =
        TransformConfig("WriteToElasticsearch6", SpecMappers.CONFIG.readTree(json) as ObjectNode)

    @Test
    fun `写端配置按 snake_case 绑定并经 provider 生成 transform`() {
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
    fun `写端默认值绑定`() {
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
    fun `写端连接 uri 为空时 validate 报错`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            Elasticsearch6WriteConfig(index = "orders").validate()
        }
        assertNotNull(error.message)
    }

    @Test
    fun `写端连接 uri 必须包含合法 HTTP 节点`() {
        assertThrows(IllegalArgumentException::class.java) {
            Elasticsearch6WriteConfig(connectionUri = "http://localhost:9200,", index = "orders").validate()
        }
    }

    @Test
    fun `写端 index 为空时 validate 报错`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            Elasticsearch6WriteConfig(connectionUri = "http://localhost:9200").validate()
        }
        assertNotNull(error.message)
    }

    @Test
    fun `写端 batch_size 非正时 validate 报错`() {
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
    fun `合法写端配置 validate 不抛异常`() {
        Elasticsearch6WriteConfig(
            connectionUri = "http://localhost:9200,http://node2:9200",
            index = "orders",
            batchSize = 200,
        ).validate()
    }

    @Test
    fun `重复 schema 字段会被拒绝`() {
        assertThrows(IllegalArgumentException::class.java) {
            Elasticsearch6WriteConfig(
                connectionUri = "http://localhost:9200",
                index = "orders",
                schemaFields = listOf("id:INT64", "id:STRING"),
            ).validate()
        }
    }
}
