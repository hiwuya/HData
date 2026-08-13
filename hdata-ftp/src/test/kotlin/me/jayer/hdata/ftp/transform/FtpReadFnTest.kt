package me.jayer.hdata.ftp.transform

import io.opentelemetry.context.Context
import me.jayer.hdata.ftp.FtpConnection
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
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
import java.util.Collections

class FtpReadFnTest {

    private fun newFn(): FtpReadFn =
        FtpReadFn(
            connection = FtpConnection(host = "h", port = 21, user = "u", username = "", password = ""),
            fileFormat = "text",
            schemaFields = null,
            encoding = "UTF-8",
            outputSchema = Schema.builder()
                .addNullableField("content", Schema.FieldType.STRING)
                .build(),
        )

    private class CollectingOutputBuilder<T>(private val value: T) : OutputBuilder<T> {
        override fun setValue(value: T): OutputBuilder<T> = this
        override fun setTimestamp(timestamp: Instant): OutputBuilder<T> = this
        override fun setWindow(window: BoundedWindow): OutputBuilder<T> = this
        override fun setWindows(windows: MutableCollection<out BoundedWindow>): OutputBuilder<T> = this
        override fun setPaneInfo(paneInfo: org.apache.beam.sdk.transforms.windowing.PaneInfo): OutputBuilder<T> = this
        override fun setRecordId(recordId: String?): OutputBuilder<T> = this
        override fun setRecordOffset(recordOffset: Long?): OutputBuilder<T> = this
        override fun setCausedByDrain(causedByDrain: CausedByDrain): OutputBuilder<T> = this
        override fun setOpenTelemetryContext(openTelemetryContext: Context?): OutputBuilder<T> = this
        override fun setValueKind(valueKind: ValueKind): OutputBuilder<T> = this
        override fun output() {}
        override fun getValue(): T = value
        override fun getTimestamp(): Instant = Instant.now()
        override fun getWindows(): MutableCollection<out BoundedWindow> = Collections.emptyList()
        override fun getPaneInfo(): org.apache.beam.sdk.transforms.windowing.PaneInfo =
            org.apache.beam.sdk.transforms.windowing.PaneInfo.NO_FIRING
        override fun getRecordId(): String? = null
        override fun getOpenTelemetryContext(): Context? = null
        override fun getRecordOffset(): Long? = null
        override fun causedByDrain(): CausedByDrain = CausedByDrain.NORMAL
        override fun getValueKind(): ValueKind = ValueKind.INSERT
        override fun explodeWindows(): MutableIterable<out WindowedValue<T>> = Collections.emptyList()
        override fun <OtherT : Any> withValue(value: OtherT): WindowedValue<OtherT> =
            throw UnsupportedOperationException()
    }

    private class CollectingReceiver : DoFn.OutputReceiver<OffsetRange> {
        val collected = mutableListOf<OffsetRange>()
        override fun output(output: OffsetRange) {
            collected.add(output)
        }
        override fun outputWithTimestamp(output: OffsetRange, timestamp: Instant) {
            collected.add(output)
        }
        override fun builder(value: OffsetRange): OutputBuilder<OffsetRange> = CollectingOutputBuilder(value)
    }

    @Test
    fun `getInitialRestriction returns full-unit range`() {
        val fn = newFn()
        val restriction = fn.getInitialRestriction("file.txt")
        assertEquals(OffsetRange(0, 1), restriction)
    }

    @Test
    fun `splitRestriction on a positive-range yields single contiguous covering range`() {
        val fn = newFn()
        val receiver = CollectingReceiver()
        fn.splitRestriction("file.txt", OffsetRange(0, 1000), receiver)

        assertTrue(receiver.collected.isNotEmpty(), "split should produce at least one range")
        val ranges = receiver.collected.sortedBy { it.from }

        // sorted by from
        assertEquals(ranges, receiver.collected, "output must be sorted by from")

        // contiguous and covers whole range
        var cursor = 0L
        for (r in ranges) {
            assertEquals(cursor, r.from, "ranges must be contiguous")
            cursor = r.to
        }
        assertEquals(1000L, cursor, "ranges must cover the whole input range")

        // a large range must be subdivided into more than one piece
        assertTrue(ranges.size > 1, "large range should be split into multiple sub-ranges")

        // empty-range yields nothing
        val emptyReceiver = CollectingReceiver()
        fn.splitRestriction("file.txt", OffsetRange(0, 0), emptyReceiver)
        assertTrue(emptyReceiver.collected.isEmpty(), "empty restriction yields no splits")
    }

    @Test
    fun `newTracker currentRestriction equals input and coder non-null`() {
        val fn = newFn()
        val restriction = OffsetRange(0, 1)
        val tracker: RestrictionTracker<OffsetRange, Long> = fn.newTracker(restriction)
        assertEquals(restriction, tracker.currentRestriction())

        assertNotNull(fn.restrictionCoder())
    }
}
