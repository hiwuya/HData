package me.jayer.hdata.filesystem

import me.jayer.hdata.filesystem.transform.FilesystemReadFn
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.CausedByDrain
import org.apache.beam.sdk.values.OutputBuilder
import org.apache.beam.sdk.values.ValueKind
import org.apache.beam.sdk.values.WindowedValue
import org.joda.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * `FilesystemReadFn` 的 Splittable DoFn 受限拆分行为。
 *
 * 本模块读路径的限制是 `OffsetRange(0,1)` 表示"整个文件一段"，[FilesystemReadFn.splitRestriction]
 * 并不真正做字节级切分（文件按行读出，整文件作为一个 element 处理），因此对任意输入限制只产出一份
 * 与输入完全一致的 `OffsetRange`。测试据此校验拆分契约（覆盖完整区间、有序、连续）。
 */
class FilesystemReadFnTest {

    private val fn = FilesystemReadFn(
        FilesystemReadConfig(path = "/tmp/x"),
        Schema.builder().addStringField("content").build(),
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
        override fun getPaneInfo(): PaneInfo = PaneInfo.NO_FIRING
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
        override fun setWindows(w: Collection<out BoundedWindow>): OutputBuilder<OffsetRange> = this
        override fun setPaneInfo(p: PaneInfo): OutputBuilder<OffsetRange> = this
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
        override fun getPaneInfo(): PaneInfo = PaneInfo.NO_FIRING
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
    fun `splitRestriction covers the whole range in one partition`() {
        val restriction = OffsetRange(0, 1000)
        val receiver = CollectingReceiver()
        fn.splitRestriction("/tmp/x", restriction, receiver)

        assertTrue(receiver.outputs.isNotEmpty())
        assertEquals(1, receiver.outputs.size)
        assertEquals(restriction, receiver.outputs[0])
        assertEquals(restriction.from, receiver.outputs[0].from)
        assertEquals(restriction.to, receiver.outputs[0].to)
    }

    @Test
    fun `splitRestriction handles empty range`() {
        val restriction = OffsetRange(0, 0)
        val receiver = CollectingReceiver()
        fn.splitRestriction("/tmp/x", restriction, receiver)

        assertEquals(1, receiver.outputs.size)
        assertEquals(restriction, receiver.outputs[0])
    }

    @Test
    fun `newTracker currentRestriction equals restriction`() {
        val restriction = OffsetRange(0, 1)
        val tracker = fn.newTracker(restriction)
        assertEquals(restriction, tracker.currentRestriction())
    }

    @Test
    fun `restrictionCoder is non-null`() {
        assertNotNull(fn.restrictionCoder())
    }

    @Test
    fun `getInitialRestriction is the single-file range`() {
        assertEquals(OffsetRange(0, 1), fn.getInitialRestriction("/tmp/x"))
    }
}
