package me.jayer.hdata.elasticsearch6

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode

/**
 * 验证 [Elasticsearch6ReadConfig] 的 snake_case 绑定与 [Elasticsearch6ReadConfig.validate]。
 *
 * 不连接真实 ES 集群：这里只校验配置解析与约束。
 */
class Elasticsearch6ReadConfigTest {

    private fun transformConfig(json: String): TransformConfig =
        TransformConfig("ReadFromElasticsearch6", SpecMappers.CONFIG.readTree(json) as ObjectNode)

    @Test
    fun `读端配置按 snake_case 绑定并经 provider 生成 transform`() {
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
    fun `读端多索引与默认值绑定`() {
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
    fun `读端连接 uri 为空时 validate 报错`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            Elasticsearch6ReadConfig(index = "orders").validate()
        }
        assertNotNull(error.message)
    }

    @Test
    fun `读端连接 uri 必须包含合法 HTTP 节点`() {
        listOf("localhost:9200", "ftp://localhost:9200").forEach { uri ->
            assertThrows(IllegalArgumentException::class.java) {
                Elasticsearch6ReadConfig(connectionUri = uri, index = "orders").validate()
            }
        }
    }

    @Test
    fun `读端 index 与 indices 都缺时 validate 报错`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            Elasticsearch6ReadConfig(connectionUri = "http://localhost:9200").validate()
        }
        assertNotNull(error.message)
    }

    @Test
    fun `读端 scroll_size 非正时 validate 报错`() {
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
    fun `合法读端配置 validate 不抛异常`() {
        Elasticsearch6ReadConfig(
            connectionUri = "http://localhost:9200",
            index = "orders",
            scrollSize = 100,
        ).validate()
    }

    @Test
    fun `index 与 indices 互斥且列表不收空值`() {
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
    }
}
