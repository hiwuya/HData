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
 * startup / bounded 模式到每分区起止偏移量的翻译。
 *
 * 用 Kafka 自带的 [MockConsumer]，不需要真 broker。这一层是整个读取端唯一还由我们自己负责的逻辑
 * （读取本身交给了 Beam 的 `ReadFromKafkaDoFn`），算错了就会漏读或重读，所以覆盖得细一些。
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
            // MockConsumer.committed() 对没 assign 过的分区一律返回 OffsetAndMetadata(0)，
            // 真的 KafkaConsumer 没这个限制（committed 是问组协调器要的），这里只是配合夹具
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
    fun `earliest 到 latest 覆盖每个分区的完整区间`() {
        val descriptors = KafkaOffsets.ranges(config(), consumer())

        assertEquals(2, descriptors.size)
        assertEquals(listOf(0, 1), descriptors.map { it.partition.partition() })
    }

    @Test
    fun `latest-offset 起点等于终点时该分区被整个跳过`() {
        // 起点和终点都取当前末尾，没有任何可读数据，不该产出空转的读取单元
        val descriptors = KafkaOffsets.ranges(
            config(startup = KafkaReadConfig.LATEST_OFFSET),
            consumer(),
        )

        assertTrue(descriptors.isEmpty())
    }

    @Test
    fun `unbounded 模式不设终点`() {
        val descriptors = KafkaOffsets.ranges(
            config(bounded = KafkaReadConfig.UNBOUNDED),
            consumer(),
        )

        assertEquals(2, descriptors.size)
        assertNull(descriptors.first().stop)
    }

    @Test
    fun `group-offsets 用已提交的偏移量做起点`() {
        val descriptors = KafkaOffsets.ranges(
            config(startup = KafkaReadConfig.GROUP_OFFSETS),
            consumer(committed = mapOf(tp0 to 30L, tp1 to 20L)),
        )

        assertEquals(listOf(30L, 20L), descriptors.map { it.start })
    }

    @Test
    fun `group-offsets 遇到没提交过的分区回退到最早偏移量`() {
        // 只提交了 0 号分区；1 号分区必须回退，不能直接漏掉
        val descriptors = KafkaOffsets.ranges(
            config(startup = KafkaReadConfig.GROUP_OFFSETS),
            consumer(beginning = mapOf(tp0 to 5L, tp1 to 7L), committed = mapOf(tp0 to 30L)),
        )

        assertEquals(listOf(30L, 7L), descriptors.map { it.start })
    }

    @Test
    fun `specific-offsets 按 topic 冒号 partition 取值`() {
        val descriptors = KafkaOffsets.ranges(
            config(startup = KafkaReadConfig.SPECIFIC_OFFSETS) {
                copy(scanStartupSpecificOffsets = mapOf("orders:0" to 11L, "orders:1" to 22L))
            },
            consumer(),
        )

        assertEquals(listOf(11L, 22L), descriptors.map { it.start })
    }

    @Test
    fun `specific-offsets 漏配某个分区时直接报错`() {
        // 静默跳过或静默从头读都会让数据对不上，这里必须拦住
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
    fun `bounded specific-offsets 决定终点`() {
        val descriptors = KafkaOffsets.ranges(
            config(bounded = KafkaReadConfig.SPECIFIC_OFFSETS) {
                copy(scanBoundedSpecificOffsets = mapOf("orders:0" to 40L, "orders:1" to 10L))
            },
            consumer(),
        )

        assertEquals(listOf(40L, 10L), descriptors.map { it.stop })
    }

    @Test
    fun `topic_pattern 按正则匹配 topic`() {
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
    fun `topic 不存在时报错而不是静默读出零条`() {
        val error = assertFailsWith<IllegalArgumentException> {
            KafkaOffsets.ranges(
                KafkaReadConfig(bootstrapServers = "localhost:9092", topics = listOf("nope")),
                consumer(),
            )
        }
        assertTrue("nope" in error.message!!)
    }

    @Test
    fun `消费者属性关掉自动提交`() {
        val props = KafkaOffsets.consumerProperties(config())

        assertEquals("false", props["enable.auto.commit"])
        assertEquals("g1", props["group.id"])
    }

    @Test
    fun `用户属性可以覆盖默认值`() {
        val props = KafkaOffsets.consumerProperties(
            config { copy(properties = mapOf("auto.offset.reset" to "earliest", "security.protocol" to "SSL")) }
        )

        assertEquals("earliest", props["auto.offset.reset"])
        assertEquals("SSL", props["security.protocol"])
    }

    @Test
    fun `用户属性不能打开自动提交`() {
        val props = KafkaOffsets.consumerProperties(
            config { copy(properties = mapOf("enable.auto.commit" to "true")) }
        )

        assertEquals("false", props["enable.auto.commit"])
    }

}
