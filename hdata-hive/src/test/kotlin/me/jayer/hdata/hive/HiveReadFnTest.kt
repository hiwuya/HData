package me.jayer.hdata.hive

import io.opentelemetry.context.Context
import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.CausedByDrain
import org.apache.beam.sdk.values.OutputBuilder
import org.apache.beam.sdk.values.ValueKind
import org.apache.beam.sdk.values.WindowedValue
import org.apache.beam.sdk.values.WindowedValues
import org.joda.time.Instant
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode

/**
 * 针对 [HiveReadFn] 的 Splittable DoFn 拆分逻辑做纯单元验证——
 * 不连接真实 HiveServer2。`splitRestriction` 仅拆分限制（restriction），不需要数据库连接。
 */
class HiveReadFnTest {

    /** 收集 `splitRestriction` 输出（经由 [OutputBuilder.output]）的 [OutputReceiver] 实现。 */
    private class CollectingReceiver(collected: MutableList<OffsetRange>) :
        DoFn.OutputReceiver<OffsetRange> {
        val collected: MutableList<OffsetRange> = collected
        override fun builder(value: OffsetRange): OutputBuilder<OffsetRange> =
            CollectingOutputBuilder(this.collected, value)
    }

    /** 最小可用的 [OutputBuilder]，在 [output] 时把值交回收集列表。 */
    private class CollectingOutputBuilder(
        private val collected: MutableList<OffsetRange>,
        private var value: OffsetRange,
    ) : OutputBuilder<OffsetRange> {
        private var ts: Instant = Instant.now()
        private var windows: Collection<out BoundedWindow> = emptyList()
        private var pane: PaneInfo = PaneInfo.NO_FIRING

        override fun setValue(v: OffsetRange) = apply { value = v }
        override fun setTimestamp(t: Instant) = apply { ts = t }
        override fun setWindow(w: BoundedWindow) = apply { windows = listOf(w) }
        override fun setWindows(w: Collection<out BoundedWindow>) = apply { windows = w }
        override fun setPaneInfo(p: PaneInfo) = apply { pane = p }
        override fun setRecordId(r: String?) = this
        override fun setRecordOffset(o: Long?) = this
        override fun setCausedByDrain(c: CausedByDrain) = this
        override fun setOpenTelemetryContext(c: Context?) = this
        override fun setValueKind(k: ValueKind) = this
        override fun output() { collected.add(value) }

        override fun getValue(): OffsetRange = value
        override fun getTimestamp(): Instant = ts
        override fun getWindows(): Collection<out BoundedWindow> = windows
        override fun getPaneInfo(): PaneInfo = pane
        override fun getRecordId(): String? = null
        override fun getOpenTelemetryContext(): Context = Context.root()
        override fun getRecordOffset(): Long? = null
        override fun causedByDrain(): CausedByDrain = CausedByDrain.NORMAL
        override fun getValueKind(): ValueKind = ValueKind.INSERT
        override fun explodeWindows(): Iterable<out WindowedValue<OffsetRange>> = listOf(this)
        override fun <OtherT> withValue(o: OtherT): WindowedValue<OtherT> =
            WindowedValues.valueInGlobalWindow(o)
    }

    private fun schema(): Schema =
        Schema.builder().addNullableField("id", Schema.FieldType.INT64).build()

    private fun fn(): HiveReadFn = HiveReadFn(
        HiveReadConfig(url = "jdbc:hive2://localhost:10000/default", table = "orders"),
        schema(),
    )

    @Test
    fun `splitRestriction 对正跨度产出覆盖全区间且连续的拆分`() {
        val restriction = OffsetRange(0, 1000)
        val receiver = CollectingReceiver(mutableListOf())
        fn().splitRestriction("", restriction, receiver)

        val outputs = receiver.collected
        assertTrue(outputs.isNotEmpty(), "正跨度应至少拆出一份")

        // 按 from 升序
        val sorted = outputs.sortedBy { it.from }
        assertEquals(outputs, sorted, "拆分结果应按 from 有序")

        // 连续
        for (i in 1 until sorted.size) {
            assertEquals(sorted[i - 1].to, sorted[i].from, "相邻拆分应当首尾相接")
        }

        // 覆盖整个区间
        assertEquals(restriction.from, sorted.first().from)
        assertEquals(restriction.to, sorted.last().to)
    }

    @Test
    fun `splitRestriction 对空区间不产出任何拆分`() {
        val receiver = CollectingReceiver(mutableListOf())
        fn().splitRestriction("", OffsetRange(0, 0), receiver)
        assertEquals(0, receiver.collected.size)

        val receiver2 = CollectingReceiver(mutableListOf())
        fn().splitRestriction("", OffsetRange(5, 5), receiver2)
        assertEquals(0, receiver2.collected.size)
    }

    @Test
    fun `newTracker 的 currentRestriction 等于传入的限制`() {
        val restriction = OffsetRange(0, 1000)
        val tracker: OffsetRangeTracker = fn().newTracker(restriction)
        assertEquals(restriction, tracker.currentRestriction())
    }

    @Test
    fun `restrictionCoder 非空且可序列化`() {
        val coder = fn().restrictionCoder()
        assertNotNull(coder)
        assertNotNull(coder.encodedTypeDescriptor)
    }
}
