package me.jayer.hdata.elasticsearch8

import me.jayer.hdata.elasticsearch8.transform.EsReadFn
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * 验证 [EsReadFn] 的 Splittable DoFn 切分逻辑（不连接真实 ES 集群）。
 *
 * 每个 index 的限制固定为 [OffsetRange](0, 1)，这里直接构造带正 span 的限制验证切分行为。
 */
class EsReadFnTest {

    private val schema: Schema = buildSchema(emptyList())

    private val fn = EsReadFn(
        config = EsReadConfig(connectionUri = "http://localhost:9200", index = "orders"),
        schema = schema,
        schemaFields = emptyList(),
    )

    private val element = "orders"

    private class CollectingReceiver : DoFn.OutputReceiver<OffsetRange> {
        val outputs = mutableListOf<OffsetRange>()
        override fun output(output: OffsetRange) {
            outputs.add(output)
        }

        override fun builder(value: OffsetRange): org.apache.beam.sdk.values.OutputBuilder<OffsetRange> {
            throw UnsupportedOperationException("builder 不在本测试中使用")
        }
    }

    @Test
    fun `大区间被切分为多段`() {
        val restriction = OffsetRange(0, 1000)
        val receiver = CollectingReceiver()
        fn.splitRestriction(element, restriction, receiver)

        val splits = receiver.outputs
        assertTrue(splits.isNotEmpty(), "切分结果不应为空")

        // 按 from 排序
        val sorted = splits.sortedBy { it.from }
        assertEquals(sorted, splits, "切分结果应按 from 有序")

        // 连续且覆盖整段
        assertEquals(restriction.from, sorted.first().from)
        assertEquals(restriction.to, sorted.last().to)
        for (i in 1 until sorted.size) {
            assertEquals(sorted[i - 1].to, sorted[i].from, "相邻区间应首尾相接")
        }

        // 大区间应被切成多段
        assertTrue(splits.size > 1, "大区间应切成多段，实际 ${splits.size}")
    }

    @Test
    fun `空区间不产出任何切分`() {
        val restriction = OffsetRange(0, 0)
        val receiver = CollectingReceiver()
        fn.splitRestriction(element, restriction, receiver)

        assertTrue(receiver.outputs.isEmpty(), "span<=0 不应产出切分")
    }

    @Test
    fun `newTracker 的当前限制等于原限制`() {
        val restriction = OffsetRange(0, 1000)
        val tracker = fn.newTracker(restriction)
        assertEquals(restriction, tracker.currentRestriction())
    }

    @Test
    fun `restrictionCoder 非空`() {
        assertNotNull(fn.restrictionCoder())
    }
}
