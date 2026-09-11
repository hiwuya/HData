package me.jayer.hdata.kafka

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.apache.beam.sdk.util.SerializableUtils
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * snake_case binding and validation of [KafkaReadConfig].
 *
 * @author wuya
 */
class KafkaReadConfigTest {

    private fun bind(json: String): KafkaReadConfig =
        TransformConfig("ReadFromKafka", SpecMappers.CONFIG.readTree(json) as ObjectNode)
            .bind(KafkaReadConfig::class.java)

    private val minimal = KafkaReadConfig(bootstrapServers = "localhost:9092", topics = listOf("orders"))

    @Test
    fun `config binds in snake_case`() {
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
    fun `the default is a bounded snapshot that finishes when done`() {
        // Aligning with Flink would default to unbounded, but HData is mainly used for batch synchronization,
        // and defaulting to a streaming job that never ends is far too easy to trip over
        val config = bind("""{"bootstrap_servers": "localhost:9092", "topics": ["orders"]}""")

        assertEquals(KafkaReadConfig.EARLIEST_OFFSET, config.scanStartupMode)
        assertEquals(KafkaReadConfig.LATEST_OFFSET, config.scanBoundedMode)
        assertTrue(config.bounded)
        assertEquals("string", config.valueFormat)
    }

    @Test
    fun `with unbounded the bounded flag is false`() {
        assertFalse(minimal.copy(scanBoundedMode = KafkaReadConfig.UNBOUNDED).bounded)
    }

    @Test
    fun `an empty bootstrap_servers fails validation`() {
        assertFailsWith<IllegalArgumentException> { KafkaReadConfig(topics = listOf("orders")).validate() }
    }

    @Test
    fun `exactly one of topics and topic_pattern must be filled in`() {
        assertFailsWith<IllegalArgumentException> {
            KafkaReadConfig(bootstrapServers = "localhost:9092").validate()
        }
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(topicPattern = "ord.*").validate()
        }
        KafkaReadConfig(bootstrapServers = "localhost:9092", topicPattern = "ord.*").validate()
        assertFailsWith<IllegalArgumentException> { minimal.copy(topics = listOf("orders", "orders")).validate() }
    }

    @Test
    fun `an invalid mode value fails and lists the valid values`() {
        val startup = assertFailsWith<IllegalArgumentException> { minimal.copy(scanStartupMode = "wat").validate() }
        assertTrue("group-offsets" in startup.message!!)

        val bounded = assertFailsWith<IllegalArgumentException> { minimal.copy(scanBoundedMode = "wat").validate() }
        assertTrue("unbounded" in bounded.message!!)
    }

    @Test
    fun `timestamp mode missing its timestamp fails validation`() {
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(scanStartupMode = KafkaReadConfig.TIMESTAMP).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(scanBoundedMode = KafkaReadConfig.TIMESTAMP).validate()
        }
        minimal.copy(scanStartupMode = KafkaReadConfig.TIMESTAMP, scanStartupTimestampMillis = 1L).validate()
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(
                scanStartupMode = KafkaReadConfig.TIMESTAMP,
                scanStartupTimestampMillis = 20,
                scanBoundedMode = KafkaReadConfig.TIMESTAMP,
                scanBoundedTimestampMillis = 10,
            ).validate()
        }
    }

    @Test
    fun `properties cannot override the read-side reserved config`() {
        listOf("bootstrap.servers", "group.id", "key.deserializer", "value.deserializer", "enable.auto.commit")
            .forEach { key ->
                assertFailsWith<IllegalArgumentException> {
                    minimal.copy(properties = mapOf(key to "overridden")).validate()
                }
            }
    }

    @Test
    fun `specific-offsets mode missing its offsets fails validation`() {
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(scanStartupMode = KafkaReadConfig.SPECIFIC_OFFSETS).validate()
        }
        minimal.copy(
            scanStartupMode = KafkaReadConfig.SPECIFIC_OFFSETS,
            scanStartupSpecificOffsets = mapOf("orders:0" to 5L),
        ).validate()
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(
                scanStartupMode = KafkaReadConfig.SPECIFIC_OFFSETS,
                scanStartupSpecificOffsets = mapOf("orders:not-a-partition" to 1L),
            ).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(
                scanStartupMode = KafkaReadConfig.SPECIFIC_OFFSETS,
                scanStartupSpecificOffsets = mapOf("orders:0" to -1L),
            ).validate()
        }
    }

    @Test
    fun `parameters that do not match the offset mode and would never take effect are rejected`() {
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(scanStartupSpecificOffsets = mapOf("orders:0" to 1L)).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(scanBoundedSpecificOffsets = mapOf("orders:0" to 2L)).validate()
        }
        assertFailsWith<IllegalArgumentException> { minimal.copy(scanStartupTimestampMillis = 1L).validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(scanBoundedTimestampMillis = 2L).validate() }
    }

    @Test
    fun `both group-offsets and committing offsets require group_id`() {
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(scanStartupMode = KafkaReadConfig.GROUP_OFFSETS).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(commitOffsetsOnCheckpoint = true).validate()
        }
        minimal.copy(scanStartupMode = KafkaReadConfig.GROUP_OFFSETS, groupId = "g1").validate()
    }

    @Test
    fun `an unrecognized format name fails validation`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(valueFormat = "avro").validate() }
    }

    @Test
    fun `empty topic, invalid regex and empty broker are all rejected`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(topics = listOf("orders", " ")).validate() }
        assertFailsWith<IllegalArgumentException> {
            KafkaReadConfig(bootstrapServers = "localhost:9092", topicPattern = "[").validate()
        }
        assertFailsWith<IllegalArgumentException> { minimal.copy(bootstrapServers = "localhost:9092,").validate() }
    }

    @Test
    fun `the transform produced by the read provider can be serialized and shipped`() {
        val transform = KafkaReadProvider().from(
            TransformConfig(
                "ReadFromKafka",
                SpecMappers.CONFIG.readTree("""{"bootstrap_servers": "localhost:9092", "topics": ["orders"]}""") as ObjectNode,
            )
        )

        assertNotNull(transform)
        // The read-side DoFn ships along with the transform; capturing a non-serializable object blows up at submission time
        SerializableUtils.ensureSerializable(transform)
    }
}
