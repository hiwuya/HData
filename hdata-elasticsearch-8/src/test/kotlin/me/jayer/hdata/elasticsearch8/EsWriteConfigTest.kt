package me.jayer.hdata.elasticsearch8

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode

/**
 * 验证 [EsWriteConfig] 的 snake_case 绑定与 [EsWriteConfig.validate]。
 */
class EsWriteConfigTest {

    private fun transformConfig(json: String): TransformConfig =
        TransformConfig("WriteToElasticsearch8", SpecMappers.CONFIG.readTree(json) as ObjectNode)

    @Test
    fun `写端配置按 snake_case 绑定并经 provider 生成 transform`() {
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
    fun `写端未知配置键会报错`() {
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
    fun `写端 connection_uri 为空时报错`() {
        assertThrows(IllegalArgumentException::class.java) {
            EsWriteConfig(index = "orders").validate()
        }
    }

    @Test
    fun `写端 index 为空时报错`() {
        assertThrows(IllegalArgumentException::class.java) {
            EsWriteConfig(connectionUri = "http://localhost:9200").validate()
        }
    }

    @Test
    fun `写端 batch_size 非正时报错`() {
        assertThrows(IllegalArgumentException::class.java) {
            EsWriteConfig(connectionUri = "http://localhost:9200", index = "orders", batchSize = 0).validate()
        }
    }

    @Test
    fun `合法写端配置 validate 不抛异常`() {
        EsWriteConfig(connectionUri = "http://localhost:9200", index = "orders").validate()
    }
}
