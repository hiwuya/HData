package me.jayer.hdata.mongodb.transform

import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.values.CausedByDrain
import org.apache.beam.sdk.values.OutputBuilder
import org.apache.beam.sdk.values.ValueKind
import org.apache.beam.sdk.values.WindowedValue
import org.joda.time.Instant
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class MongoReadFnTest {

    private val fn = MongoReadFn(
        connectionUri = "mongodb://localhost:27017",
        database = "mydb",
        collection = "orders",
        schemaFields = emptyList(),
        fetchSize = 100,
    )

    private val element = MongoReadSplit(
        connectionUri = "mongodb://localhost:27017",
        database = "mydb",
        collection = "orders",
    )

    private class CollectingReceiver : DoFn.OutputReceiver<OffsetRange> {
        val outputs = mutableListOf<OffsetRange>()

        override fun builder(value: OffsetRange): OutputBuilder<OffsetRange> =
            CollectingOutputBuilder(value, outputs)
    }

    private class CollectingOutputBuilder(
        private val value: OffsetRange,
        private val sink: MutableList<OffsetRange>,
    ) : OutputBuilder<OffsetRange> {
        override fun output() {
            sink.add(value)
        }

        override fun getValue(): OffsetRange = value
        override fun getTimestamp(): Instant = Instant.EPOCH
        override fun getWindows(): Collection<BoundedWindow> = emptyList()
        override fun getPaneInfo(): org.apache.beam.sdk.transforms.windowing.PaneInfo =
            org.apache.beam.sdk.transforms.windowing.PaneInfo.NO_FIRING
        override fun getRecordId(): String = ""
        override fun getOpenTelemetryContext(): io.opentelemetry.context.Context =
            io.opentelemetry.context.Context.root()
        override fun getRecordOffset(): Long = 0L
        override fun causedByDrain(): CausedByDrain = CausedByDrain.NORMAL
        override fun getValueKind(): ValueKind = ValueKind.INSERT
        override fun explodeWindows(): Iterable<WindowedValue<OffsetRange>> = listOf(this)

        override fun setValue(v: OffsetRange): OutputBuilder<OffsetRange> = this
        override fun setTimestamp(t: Instant): OutputBuilder<OffsetRange> = this
        override fun setWindow(w: BoundedWindow): OutputBuilder<OffsetRange> = this
        override fun setWindows(w: Collection<BoundedWindow>): OutputBuilder<OffsetRange> = this
        override fun setPaneInfo(p: org.apache.beam.sdk.transforms.windowing.PaneInfo): OutputBuilder<OffsetRange> = this
        override fun setRecordId(r: String?): OutputBuilder<OffsetRange> = this
        override fun setRecordOffset(l: Long?): OutputBuilder<OffsetRange> = this
        override fun setCausedByDrain(c: CausedByDrain): OutputBuilder<OffsetRange> = this
        override fun setOpenTelemetryContext(c: io.opentelemetry.context.Context?): OutputBuilder<OffsetRange> = this
        override fun setValueKind(k: ValueKind): OutputBuilder<OffsetRange> = this

        override fun <OtherT> withValue(o: OtherT): WindowedValue<OtherT> = StubWindowedValue(o)
    }

    private class StubWindowedValue<T>(private val value: T) : WindowedValue<T> {
        override fun getValue(): T = value
        override fun getTimestamp(): Instant = Instant.EPOCH
        override fun getWindows(): Collection<BoundedWindow> = emptyList()
        override fun getPaneInfo(): org.apache.beam.sdk.transforms.windowing.PaneInfo =
            org.apache.beam.sdk.transforms.windowing.PaneInfo.NO_FIRING
        override fun getRecordId(): String = ""
        override fun getOpenTelemetryContext(): io.opentelemetry.context.Context =
            io.opentelemetry.context.Context.root()
        override fun getRecordOffset(): Long = 0L
        override fun causedByDrain(): CausedByDrain = CausedByDrain.NORMAL
        override fun getValueKind(): ValueKind = ValueKind.INSERT
        override fun explodeWindows(): Iterable<WindowedValue<T>> = listOf(this)

        override fun <OtherT> withValue(o: OtherT): WindowedValue<OtherT> = StubWindowedValue(o)
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
        assertTrue(splits.size > 1, "1000 条 / 每段 100 应切成多段，实际 ${splits.size}")
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
