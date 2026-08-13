package me.jayer.hdata.kafka.transform

import me.jayer.hdata.kafka.KafkaTopicPartition
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.ArrayList

/** 收集 [OffsetRange] 输出的 [DoFn.OutputReceiver] 实现。 */
private class CollectingReceiver(private val sink: MutableList<OffsetRange>) : DoFn.OutputReceiver<OffsetRange> {
    override fun output(output: OffsetRange) {
        sink.add(output)
    }

    override fun outputWithTimestamp(output: OffsetRange, timestamp: org.joda.time.Instant) {
        sink.add(output)
    }

    override fun builder(value: OffsetRange): org.apache.beam.sdk.values.OutputBuilder<OffsetRange> =
        throw UnsupportedOperationException("builder 不在 splitRestriction 路径上使用")
}

/**
 * 验证 [KafkaReadFn] 的 Splittable DoFn 能力：切分限制、tracker、coder。
 * 这些路径都不触碰 Kafka consumer（只调用 [OffsetRange.split]）。
 */
class KafkaReadFnTest {

    private fun newFn(offsetSplitSize: Long = 100): KafkaReadFn =
        KafkaReadFn(
            bootstrapServers = "localhost:9092",
            groupId = "test-group",
            consumerConfig = emptyMap(),
            startupMode = "earliest-offset",
            specificOffsets = emptyMap(),
            startupTimestampMillis = null,
            offsetSplitSize = offsetSplitSize,
        )

    @Test
    fun `splitRestriction 把大区间切成连续且覆盖完整的小段`() {
        val fn = newFn(offsetSplitSize = 100)
        val element = KafkaTopicPartition("t", 0)
        val restriction = OffsetRange(0, 1000)
        val outputs = ArrayList<OffsetRange>()
        fn.splitRestriction(element, restriction, CollectingReceiver(outputs))

        assertTrue(outputs.isNotEmpty(), "切分结果不应为空")
        assertTrue(outputs.size > 1, "区间应被切成多段, 实际=${outputs.size}")

        val sorted = outputs.sortedBy { it.from }
        // 从头到尾连续覆盖
        assertEquals(0, sorted.first().from)
        assertEquals(1000, sorted.last().to)
        for (i in 1 until sorted.size) {
            assertEquals(sorted[i - 1].to, sorted[i].from, "相邻段之间必须连续")
        }
    }

    @Test
    fun `splitRestriction 对空区间输出为空`() {
        val fn = newFn()
        val element = KafkaTopicPartition("t", 0)
        val restriction = OffsetRange(0, 0)
        val outputs = ArrayList<OffsetRange>()
        fn.splitRestriction(element, restriction, CollectingReceiver(outputs))

        assertTrue(outputs.isEmpty(), "空区间不应产生任何切分")
    }

    @Test
    fun `newTracker 返回与限制一致的 OffsetRangeTracker`() {
        val fn = newFn()
        val tracker: OffsetRangeTracker = fn.newTracker(OffsetRange(0, 100))
        assertEquals(OffsetRange(0, 100), tracker.currentRestriction())
    }

    @Test
    fun `restrictionCoder 不为空`() {
        val fn = newFn()
        val coder: Coder<OffsetRange> = fn.restrictionCoder()
        assertNotNull(coder)
    }
}
