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
 * 只把输出收进一个列表的 [DoFn.OutputReceiver]，用于直接调用 `@ProcessElement` / `@SplitRestriction` 做单测。
 *
 * Beam 的 `OutputReceiver` 不是 SAM 接口（`builder(value)` 要返回一个 `OutputBuilder`），
 * 直接写 lambda 编译不过，于是每个连接器模块的测试里都会长出上百行一模一样的桩代码。
 * 放在这里让所有模块共用。
 *
 * 放在 main 而不是 test 源码集，是因为 Maven 模块之间共享 test 类需要额外打 test-jar，
 * 而这个类本身对第三方连接器作者也有用。
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
            throw UnsupportedOperationException("测试用的收集器不支持 withValue")
    }
}
