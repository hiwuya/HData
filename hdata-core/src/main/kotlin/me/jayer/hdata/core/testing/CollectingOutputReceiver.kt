package me.jayer.hdata.core.testing

import io.opentelemetry.context.Context
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.CausedByDrain
import org.apache.beam.sdk.values.OutputBuilder
import org.apache.beam.sdk.values.ValueKind
import org.apache.beam.sdk.values.WindowedValue
import org.joda.time.Instant

/**
 * A [DoFn.OutputReceiver] that simply collects all output into a list, for unit tests that
 * invoke `@ProcessElement` / `@SplitRestriction` directly.
 *
 * Beam's `OutputReceiver` is not a SAM interface (`builder(value)` must return an `OutputBuilder`),
 * so a plain lambda will not compile, and every connector module's tests would otherwise grow
 * hundreds of lines of identical stub code. Putting it here lets all modules share it.
 *
 * It lives in the main source set rather than test, because sharing test classes between Maven
 * modules requires building an extra test-jar, and this class is also useful to third-party
 * connector authors.
 *
 * @author wuya
 */
class CollectingOutputReceiver<T> : DoFn.OutputReceiver<T> {

    private val collected = mutableListOf<T>()

    val outputs: List<T> get() = collected.toList()

    override fun builder(value: T): OutputBuilder<T> = Builder(value, collected)

    private class Builder<T>(
        private val value: T,
        private val sink: MutableList<T>,
    ) : OutputBuilder<T> {

        override fun output() {
            sink.add(value)
        }

        override fun getValue(): T = value
        override fun getTimestamp(): Instant = Instant.EPOCH
        override fun getWindows(): Collection<BoundedWindow> = emptyList()
        override fun getPaneInfo(): PaneInfo = PaneInfo.NO_FIRING
        override fun getRecordId(): String = ""
        override fun getOpenTelemetryContext(): Context = Context.root()
        override fun getRecordOffset(): Long = 0L
        override fun causedByDrain(): CausedByDrain = CausedByDrain.NORMAL
        override fun getValueKind(): ValueKind = ValueKind.INSERT
        override fun explodeWindows(): Iterable<WindowedValue<T>> = listOf(this)

        override fun setValue(v: T): OutputBuilder<T> = this
        override fun setTimestamp(t: Instant): OutputBuilder<T> = this
        override fun setWindow(w: BoundedWindow): OutputBuilder<T> = this
        override fun setWindows(w: Collection<BoundedWindow>): OutputBuilder<T> = this
        override fun setPaneInfo(p: PaneInfo): OutputBuilder<T> = this
        override fun setRecordId(r: String?): OutputBuilder<T> = this
        override fun setRecordOffset(l: Long?): OutputBuilder<T> = this
        override fun setCausedByDrain(c: CausedByDrain): OutputBuilder<T> = this
        override fun setOpenTelemetryContext(c: Context?): OutputBuilder<T> = this
        override fun setValueKind(k: ValueKind): OutputBuilder<T> = this

        override fun <OtherT> withValue(o: OtherT): WindowedValue<OtherT> =
            throw UnsupportedOperationException("The test collector does not support withValue")
    }
}
