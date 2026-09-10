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
 * 把 `@FinishBundle` 的输出收进列表的 [DoFn.FinishBundleContext]，用于直接调 `finishBundle` 做单测。
 *
 * 存在的理由和 [CollectingOutputReceiver] 一样：`FinishBundleContext` 是 `DoFn` 的**内部类**，
 * 又有两个必须实现的 `output`，每个模块各写一遍就是几百行重复代码。
 *
 * 它自己继承 `DoFn` 是 Kotlin 的一条硬约束逼出来的：`FinishBundleContext` 的构造器要一个
 * **外部 `DoFn` 实例做接收者**，Kotlin 不允许在 `super(...)` 位置传这个接收者，
 * 唯一能拿到它的办法就是让本类自己成为那个 `DoFn`。除了这点它不具备任何 DoFn 的能力。
 *
 * 除了值之外还留下**时间戳与窗口**，是为了钉住 AGENTS 里那条约定：死信必须带原始行自己的时间戳与窗口，
 * 现编 `Instant.now()` + `GlobalWindow` 在窗口化的 pipeline 里会让 `context.output` 直接抛异常，
 * 而只断言值的话这条根本测不出来。
 *
 * @author wuya
 */
class CollectingFinishBundleContext<InputT, OutputT> : DoFn<InputT, OutputT>() {

    private val collected = mutableListOf<ValueInSingleWindow<OutputT>>()
    private val tagged = mutableMapOf<TupleTag<*>, MutableList<ValueInSingleWindow<*>>>()

    val outputs: List<OutputT> get() = collected.map { it.value }
    val timestamps: List<Instant> get() = collected.map { it.timestamp }
    val windows: List<BoundedWindow> get() = collected.map { it.window }

    /** 传给被测 DoFn 的 `finishBundle`。 */
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
