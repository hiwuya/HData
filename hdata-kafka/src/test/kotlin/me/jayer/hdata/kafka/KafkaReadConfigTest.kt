package me.jayer.hdata.kafka

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode

/**
 * 验证 [KafkaReadConfig] 的 snake_case 绑定与 [KafkaReadConfig.validate]。
 */
class KafkaReadConfigTest {

    private fun transformConfig(json: String): TransformConfig =
        TransformConfig("ReadFromKafka", SpecMappers.CONFIG.readTree(json) as ObjectNode)

    @Test
    fun `读端配置按 snake_case 绑定并经 provider 生成 transform`() {
        val cfg = transformConfig(
            """
            {
              "bootstrap_servers": "localhost:9092",
              "topics": ["orders"],
              "scan_startup_mode": "earliest-offset",
              "offset_split_size": 1000
            }
            """.trimIndent()
        )

        val transform = KafkaReadProvider().from(cfg)
        assertNotNull(transform)
    }

    @Test
    fun `读端默认值`() {
        val cfg = transformConfig(
            """{"bootstrap_servers": "localhost:9092", "topics": ["orders"]}"""
        )
        val config = cfg.bind(KafkaReadConfig::class.java)

        assertEquals("earliest-offset", config.scanStartupMode)
        assertEquals(100_000, config.offsetSplitSize)
        assertEquals(emptyMap<String, String>(), config.consumerConfig)
        assertEquals(emptyMap<String, Long>(), config.scanStartupSpecificOffsets)
    }

    @Test
    fun `读端 bootstrap_servers 为空时报错`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            KafkaReadConfig(topics = listOf("orders")).validate()
        }
        assertNotNull(error.message)
    }

    @Test
    fun `读端 topics 为空时报错`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            KafkaReadConfig(bootstrapServers = "localhost:9092").validate()
        }
        assertNotNull(error.message)
    }

    @Test
    fun `读端 offset_split_size 必须为正`() {
        assertThrows(IllegalArgumentException::class.java) {
            KafkaReadConfig(bootstrapServers = "localhost:9092", topics = listOf("orders"), offsetSplitSize = 0).validate()
        }
        assertThrows(IllegalArgumentException::class.java) {
            KafkaReadConfig(bootstrapServers = "localhost:9092", topics = listOf("orders"), offsetSplitSize = -1).validate()
        }
    }

    @Test
    fun `读端 scan_startup_mode 取值非法时报错`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            KafkaReadConfig(
                bootstrapServers = "localhost:9092",
                topics = listOf("orders"),
                scanStartupMode = "wat",
            ).validate()
        }
        assertNotNull(error.message)
    }

    @Test
    fun `合法读端配置 validate 不抛异常`() {
        KafkaReadConfig(
            bootstrapServers = "localhost:9092",
            topics = listOf("orders"),
            scanStartupMode = "specific-offsets",
            scanStartupSpecificOffsets = mapOf("orders:0" to 5L),
        ).validate()
    }
}
