package me.jayer.hdata.core.testing

import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.TupleTag
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.joda.time.Instant

/**
 * A [DoFn.FinishBundleContext] that collects `@FinishBundle` output into a list, for unit tests
 * that invoke `finishBundle` directly.
 *
 * It exists for the same reason as [CollectingOutputReceiver]: `FinishBundleContext` is an **inner
 * class** of `DoFn` and has two `output` methods that must be implemented, so each module would
 * otherwise repeat hundreds of lines of boilerplate.
 *
 * It extends `DoFn` itself because of a hard Kotlin constraint: the `FinishBundleContext`
 * constructor needs an **outer `DoFn` instance as receiver**, and Kotlin does not allow passing
 * that receiver in the `super(...)` position. The only way to obtain it is to make this class
 * itself that `DoFn`. Apart from that it has no DoFn capabilities.
 *
 * Besides the value, it also records the **timestamp and window** to pin down the AGENTS
 * convention: a dead-letter record must carry the original row's own timestamp and window.
 * Fabricating `Instant.now()` + `GlobalWindow` would make `context.output` throw outright in a
 * windowed pipeline, and asserting only on the value would never catch this.
 *
 * @author wuya
 */
class CollectingFinishBundleContext<InputT, OutputT> : DoFn<InputT, OutputT>() {

    private val collected = mutableListOf<ValueInSingleWindow<OutputT>>()
    private val tagged = mutableMapOf<TupleTag<*>, MutableList<ValueInSingleWindow<*>>>()

    val outputs: List<OutputT> get() = collected.map { it.value }
    val timestamps: List<Instant> get() = collected.map { it.timestamp }
    val windows: List<BoundedWindow> get() = collected.map { it.window }

    /** The `finishBundle` context passed to the DoFn under test. */
    fun context(): DoFn<InputT, OutputT>.FinishBundleContext = Collecting()

    @Suppress("UNCHECKED_CAST")
    fun <T> taggedOutputs(tag: TupleTag<T>): List<T> =
        (tagged[tag] as? List<ValueInSingleWindow<T>>)?.map { it.value } ?: emptyList()

    private inner class Collecting : FinishBundleContext() {

        override fun getPipelineOptions(): PipelineOptions = PipelineOptionsFactory.create()

        override fun output(value: OutputT, timestamp: Instant, window: BoundedWindow) {
            collected.add(ValueInSingleWindow.of(value, timestamp, window, PaneInfo.NO_FIRING))
        }

        override fun <T> output(
            tag: TupleTag<T>,
            value: T,
            timestamp: Instant,
            window: BoundedWindow,
        ) {
            tagged.getOrPut(tag) { mutableListOf() }
                .add(ValueInSingleWindow.of(value, timestamp, window, PaneInfo.NO_FIRING))
        }
    }
}
