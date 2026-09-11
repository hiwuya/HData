package me.jayer.hdata.kafka.internal

import me.jayer.hdata.kafka.KafkaReadConfig
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Translating startup / bounded modes into per-partition start/stop offsets.
 *
 * Uses Kafka's own [MockConsumer], no real broker needed. This layer is the only logic on the read side still
 * owned by us (the reading itself is handed to Beam's `ReadFromKafkaDoFn`), and a wrong calculation means
 * missing or duplicate reads — so it's covered in detail.
 *
 * @author wuya
 */
class KafkaOffsetsTest {

    private val topic = "orders"
    private val tp0 = TopicPartition(topic, 0)
    private val tp1 = TopicPartition(topic, 1)

    private fun consumer(
        beginning: Map<TopicPartition, Long> = mapOf(tp0 to 0L, tp1 to 0L),
        end: Map<TopicPartition, Long> = mapOf(tp0 to 100L, tp1 to 50L),
        committed: Map<TopicPartition, Long> = emptyMap(),
    ): MockConsumer<ByteArray, ByteArray> {
        val c = MockConsumer<ByteArray, ByteArray>("earliest")
        val node = Node(0, "localhost", 9092)
        c.updatePartitions(
            topic,
            listOf(
                PartitionInfo(topic, 0, node, arrayOf(node), arrayOf(node)),
                PartitionInfo(topic, 1, node, arrayOf(node), arrayOf(node)),
            ),
        )
        c.updateBeginningOffsets(beginning)
        c.updateEndOffsets(end)
        if (committed.isNotEmpty()) {
            // MockConsumer.committed() always returns OffsetAndMetadata(0) for partitions it has never assigned,
            // but a real KafkaConsumer has no such limit (committed is fetched from the group coordinator); this is
            // just to work with the fixture.
            c.assign(listOf(tp0, tp1))
            c.commitSync(committed.mapValues { OffsetAndMetadata(it.value) })
        }
        return c
    }

    private fun config(
        startup: String = KafkaReadConfig.EARLIEST_OFFSET,
        bounded: String = KafkaReadConfig.LATEST_OFFSET,
        block: KafkaReadConfig.() -> KafkaReadConfig = { this },
    ) = KafkaReadConfig(
        bootstrapServers = "localhost:9092",
        topics = listOf(topic),
        groupId = "g1",
        scanStartupMode = startup,
        scanBoundedMode = bounded,
    ).block()

    @Test
    fun `earliest to latest covers the full range of every partition`() {
        val descriptors = KafkaOffsets.ranges(config(), consumer())

        assertEquals(2, descriptors.size)
        assertEquals(listOf(0, 1), descriptors.map { it.partition.partition() })
    }

    @Test
    fun `latest-offset skips the partition entirely when start equals stop`() {
        // Both the start and stop take the current end, so there is no readable data; we must not produce an idle read unit
        val descriptors = KafkaOffsets.ranges(
            config(startup = KafkaReadConfig.LATEST_OFFSET),
            consumer(),
        )

        assertTrue(descriptors.isEmpty())
    }

    @Test
    fun `unbounded mode sets no stop`() {
        val descriptors = KafkaOffsets.ranges(
            config(bounded = KafkaReadConfig.UNBOUNDED),
            consumer(),
        )

        assertEquals(2, descriptors.size)
        assertNull(descriptors.first().stop)
    }

    @Test
    fun `group-offsets uses the committed offset as the start`() {
        val descriptors = KafkaOffsets.ranges(
            config(startup = KafkaReadConfig.GROUP_OFFSETS),
            consumer(committed = mapOf(tp0 to 30L, tp1 to 20L)),
        )

        assertEquals(listOf(30L, 20L), descriptors.map { it.start })
    }

    @Test
    fun `group-offsets falls back to the earliest offset for partitions with no commit`() {
        // Only partition 0 was committed; partition 1 must fall back, it cannot simply be dropped
        val descriptors = KafkaOffsets.ranges(
            config(startup = KafkaReadConfig.GROUP_OFFSETS),
            consumer(beginning = mapOf(tp0 to 5L, tp1 to 7L), committed = mapOf(tp0 to 30L)),
        )

        assertEquals(listOf(30L, 7L), descriptors.map { it.start })
    }

    @Test
    fun `specific-offsets reads by topic colon partition`() {
        val descriptors = KafkaOffsets.ranges(
            config(startup = KafkaReadConfig.SPECIFIC_OFFSETS) {
                copy(scanStartupSpecificOffsets = mapOf("orders:0" to 11L, "orders:1" to 22L))
            },
            consumer(),
        )

        assertEquals(listOf(11L, 22L), descriptors.map { it.start })
    }

    @Test
    fun `specific-offsets errors immediately when a partition is missing`() {
        // Silently skipping or silently reading from the start would make the data mismatch; this must be caught here
        val error = assertFailsWith<IllegalArgumentException> {
            KafkaOffsets.ranges(
                config(startup = KafkaReadConfig.SPECIFIC_OFFSETS) {
                    copy(scanStartupSpecificOffsets = mapOf("orders:0" to 11L))
                },
                consumer(),
            )
        }
        assertTrue("orders:1" in error.message!!)
    }

    @Test
    fun `specific-offsets also errors when an extra non-existent partition is given`() {
        val error = assertFailsWith<IllegalArgumentException> {
            KafkaOffsets.ranges(
                config(startup = KafkaReadConfig.SPECIFIC_OFFSETS) {
                    copy(
                        scanStartupSpecificOffsets = mapOf(
                            "orders:0" to 11L,
                            "orders:1" to 22L,
                            "orders:2" to 33L,
                        )
                    )
                },
                consumer(),
            )
        }
        assertTrue("orders:2" in error.message!!)
    }

    @Test
    fun `bounded specific-offsets decides the stop`() {
        val descriptors = KafkaOffsets.ranges(
            config(bounded = KafkaReadConfig.SPECIFIC_OFFSETS) {
                copy(scanBoundedSpecificOffsets = mapOf("orders:0" to 40L, "orders:1" to 10L))
            },
            consumer(),
        )

        assertEquals(listOf(40L, 10L), descriptors.map { it.stop })
    }

    @Test
    fun `topic_pattern matches topics by regex`() {
        val c = consumer()
        val node = Node(0, "localhost", 9092)
        c.updatePartitions("other", listOf(PartitionInfo("other", 0, node, arrayOf(node), arrayOf(node))))
        c.updateEndOffsets(mapOf(TopicPartition("other", 0) to 10L))
        c.updateBeginningOffsets(mapOf(TopicPartition("other", 0) to 0L))

        val descriptors = KafkaOffsets.ranges(
            KafkaReadConfig(bootstrapServers = "localhost:9092", topicPattern = "ord.*"),
            c,
        )

        assertEquals(setOf("orders"), descriptors.map { it.partition.topic() }.toSet())
    }

    @Test
    fun `a non-existent topic errors rather than silently reading zero rows`() {
        val error = assertFailsWith<IllegalArgumentException> {
            KafkaOffsets.ranges(
                KafkaReadConfig(bootstrapServers = "localhost:9092", topics = listOf("nope")),
                consumer(),
            )
        }
        assertTrue("nope" in error.message!!)
    }

    @Test
    fun `consumer properties disable auto-commit`() {
        val props = KafkaOffsets.consumerProperties(config())

        assertEquals("false", props["enable.auto.commit"])
        assertEquals("g1", props["group.id"])
    }

    @Test
    fun `user properties can override the defaults`() {
        val props = KafkaOffsets.consumerProperties(
            config { copy(properties = mapOf("auto.offset.reset" to "earliest", "security.protocol" to "SSL")) }
        )

        assertEquals("earliest", props["auto.offset.reset"])
        assertEquals("SSL", props["security.protocol"])
    }

    @Test
    fun `user properties cannot turn auto-commit on`() {
        val props = KafkaOffsets.consumerProperties(
            config { copy(properties = mapOf("enable.auto.commit" to "true")) }
        )

        assertEquals("false", props["enable.auto.commit"])
    }
}
