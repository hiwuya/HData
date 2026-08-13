package me.jayer.hdata.kafka

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * [KafkaReadConfig] 的 snake_case 绑定与校验。
 *
 * @author wuya
 */
class KafkaReadConfigTest {

    private fun bind(json: String): KafkaReadConfig =
        TransformConfig("ReadFromKafka", SpecMappers.CONFIG.readTree(json) as ObjectNode)
            .bind(KafkaReadConfig::class.java)

    private val minimal = KafkaReadConfig(bootstrapServers = "localhost:9092", topics = listOf("orders"))

    @Test
    fun `配置按 snake_case 绑定`() {
        val config = bind(
            """
            {
              "bootstrap_servers": "localhost:9092",
              "topics": ["orders"],
              "scan_startup_mode": "timestamp",
              "scan_startup_timestamp_millis": 1700000000000,
              "scan_bounded_mode": "unbounded",
              "value_format": "raw",
              "properties": {"security.protocol": "SSL"}
            }
            """.trimIndent()
        )

        assertEquals("timestamp", config.scanStartupMode)
        assertEquals(1_700_000_000_000L, config.scanStartupTimestampMillis)
        assertEquals("raw", config.valueFormat)
        assertEquals(mapOf("security.protocol" to "SSL"), config.properties)
        config.validate()
    }

    @Test
    fun `默认是有界快照，跑完就结束`() {
        // 对齐 Flink 会把默认值设成 unbounded，但 HData 主要用于批量同步，
        // 默认跑成永不结束的流作业太容易踩坑
        val config = bind("""{"bootstrap_servers": "localhost:9092", "topics": ["orders"]}""")

        assertEquals(KafkaReadConfig.EARLIEST_OFFSET, config.scanStartupMode)
        assertEquals(KafkaReadConfig.LATEST_OFFSET, config.scanBoundedMode)
        assertTrue(config.bounded)
        assertEquals("string", config.valueFormat)
    }

    @Test
    fun `unbounded 时 bounded 标记为假`() {
        assertFalse(minimal.copy(scanBoundedMode = KafkaReadConfig.UNBOUNDED).bounded)
    }

    @Test
    fun `bootstrap_servers 为空时报错`() {
        assertFailsWith<IllegalArgumentException> { KafkaReadConfig(topics = listOf("orders")).validate() }
    }

    @Test
    fun `topics 与 topic_pattern 必须且只能填一个`() {
        assertFailsWith<IllegalArgumentException> {
            KafkaReadConfig(bootstrapServers = "localhost:9092").validate()
        }
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(topicPattern = "ord.*").validate()
        }
        KafkaReadConfig(bootstrapServers = "localhost:9092", topicPattern = "ord.*").validate()
    }

    @Test
    fun `模式取值非法时报错并列出可选值`() {
        val startup = assertFailsWith<IllegalArgumentException> { minimal.copy(scanStartupMode = "wat").validate() }
        assertTrue("group-offsets" in startup.message!!)

        val bounded = assertFailsWith<IllegalArgumentException> { minimal.copy(scanBoundedMode = "wat").validate() }
        assertTrue("unbounded" in bounded.message!!)
    }

    @Test
    fun `timestamp 模式缺时间戳时报错`() {
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(scanStartupMode = KafkaReadConfig.TIMESTAMP).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(scanBoundedMode = KafkaReadConfig.TIMESTAMP).validate()
        }
        minimal.copy(scanStartupMode = KafkaReadConfig.TIMESTAMP, scanStartupTimestampMillis = 1L).validate()
    }

    @Test
    fun `specific-offsets 模式缺偏移量时报错`() {
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(scanStartupMode = KafkaReadConfig.SPECIFIC_OFFSETS).validate()
        }
        minimal.copy(
            scanStartupMode = KafkaReadConfig.SPECIFIC_OFFSETS,
            scanStartupSpecificOffsets = mapOf("orders:0" to 5L),
        ).validate()
    }

    @Test
    fun `group-offsets 与提交偏移量都需要 group_id`() {
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(scanStartupMode = KafkaReadConfig.GROUP_OFFSETS).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(commitOffsetsOnCheckpoint = true).validate()
        }
        minimal.copy(scanStartupMode = KafkaReadConfig.GROUP_OFFSETS, groupId = "g1").validate()
    }

    @Test
    fun `格式名不认识时报错`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(valueFormat = "avro").validate() }
    }
}
