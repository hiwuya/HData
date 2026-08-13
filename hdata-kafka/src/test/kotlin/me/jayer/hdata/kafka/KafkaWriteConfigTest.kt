package me.jayer.hdata.kafka

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode

/**
 * 验证 [KafkaWriteConfig] 的 snake_case 绑定与 [KafkaWriteConfig.validate]。
 */
class KafkaWriteConfigTest {

    private fun transformConfig(json: String): TransformConfig =
        TransformConfig("WriteToKafka", SpecMappers.CONFIG.readTree(json) as ObjectNode)

    @Test
    fun `写端配置按 snake_case 绑定并经 provider 生成 transform`() {
        val cfg = transformConfig(
            """
            {
              "bootstrap_servers": "localhost:9092",
              "topic": "orders",
              "batch_size": 500
            }
            """.trimIndent()
        )

        val transform = KafkaWriteProvider().from(cfg)
        assertNotNull(transform)
    }

    @Test
    fun `写端默认值`() {
        val cfg = transformConfig(
            """{"bootstrap_servers": "localhost:9092", "topic": "orders"}"""
        )
        val config = cfg.bind(KafkaWriteConfig::class.java)

        assertEquals(1000, config.batchSize)
        assertEquals("string", config.keyFormat)
        assertEquals("string", config.valueFormat)
    }

    @Test
    fun `写端 bootstrap_servers 为空时报错`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            KafkaWriteConfig(topic = "orders").validate()
        }
        assertNotNull(error.message)
    }

    @Test
    fun `写端 topic 为空时报错`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            KafkaWriteConfig(bootstrapServers = "localhost:9092").validate()
        }
        assertNotNull(error.message)
    }

    @Test
    fun `写端 batch_size 必须为正`() {
        assertThrows(IllegalArgumentException::class.java) {
            KafkaWriteConfig(bootstrapServers = "localhost:9092", topic = "orders", batchSize = 0).validate()
        }
    }

    @Test
    fun `合法写端配置 validate 不抛异常`() {
        KafkaWriteConfig(bootstrapServers = "localhost:9092", topic = "orders", batchSize = 200).validate()
    }
}
