package me.jayer.hdata.elasticsearch6

import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.values.OutputBuilder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test

/**
 * 验证 [Elasticsearch6ReadFn] 的 Splittable DoFn 辅助方法（`splitRestriction` /
 * `newTracker` / `restrictionCoder`）不需要存活的 ES 连接。
 *
 * 注意：本读路径按"索引"粒度认领，`splitRestriction` 直接把整段 [OffsetRange]
 * 透传给下游（`OffsetRange(0,1)`），由 `@ProcessElement` 一次性 scroll 读完整个索引，
 * 因此这里断言它"不切分、整段输出"，而非像 Kafka 那样把大区间切成多段。
 * 不连接真实 ES 集群。
 */
class Elasticsearch6ReadFnTest {

    /**
     * 收集 splitRestriction 输出的 [DoFn.OutputReceiver] 实现。
     * 本测试只调 [DoFn.OutputReceiver.output]，不会用到 [builder]，后者返回留桩即可。
     */
    private class CollectingReceiver : DoFn.OutputReceiver<OffsetRange> {
        val outputs = mutableListOf<OffsetRange>()
        override fun output(output: OffsetRange) {
            outputs.add(output)
        }

        // 本测试不调用 builder(...)，返回一个最小的 OutputBuilder 桩。
        override fun builder(value: OffsetRange): OutputBuilder<OffsetRange> = StubOutputBuilder(value)
    }

    /** 仅用于满足 [DoFn.OutputReceiver.builder] 签名的 [OutputBuilder] 桩，本测试不会调用其方法。 */
    private class StubOutputBuilder(private val value: OffsetRange) : OutputBuilder<OffsetRange> {
        override fun getValue(): OffsetRange = value
        override fun getTimestamp(): org.joda.time.Instant = org.joda.time.Instant.now()
        override fun getWindows(): Collection<org.apache.beam.sdk.transforms.windowing.BoundedWindow> = emptyList()
        override fun getPaneInfo(): org.apache.beam.sdk.transforms.windowing.PaneInfo = org.apache.beam.sdk.transforms.windowing.PaneInfo.NO_FIRING
        override fun getRecordId(): String? = null
        override fun getOpenTelemetryContext(): io.opentelemetry.context.Context? = null
        override fun getRecordOffset(): Long? = null
        override fun causedByDrain(): org.apache.beam.sdk.values.CausedByDrain = org.apache.beam.sdk.values.CausedByDrain.NORMAL
        override fun getValueKind(): org.apache.beam.sdk.values.ValueKind = org.apache.beam.sdk.values.ValueKind.INSERT
        override fun explodeWindows(): Iterable<org.apache.beam.sdk.values.WindowedValue<OffsetRange>> = listOf(this)
        override fun <OtherT : Any> withValue(v: OtherT): org.apache.beam.sdk.values.WindowedValue<OtherT> =
            throw UnsupportedOperationException()

        override fun setValue(v: OffsetRange): OutputBuilder<OffsetRange> = this
        override fun setTimestamp(t: org.joda.time.Instant): OutputBuilder<OffsetRange> = this
        override fun setWindow(w: org.apache.beam.sdk.transforms.windowing.BoundedWindow): OutputBuilder<OffsetRange> = this
        override fun setWindows(w: Collection<org.apache.beam.sdk.transforms.windowing.BoundedWindow>): OutputBuilder<OffsetRange> = this
        override fun setPaneInfo(p: org.apache.beam.sdk.transforms.windowing.PaneInfo): OutputBuilder<OffsetRange> = this
        override fun setRecordId(id: String?): OutputBuilder<OffsetRange> = this
        override fun setRecordOffset(o: Long?): OutputBuilder<OffsetRange> = this
        override fun setCausedByDrain(c: org.apache.beam.sdk.values.CausedByDrain): OutputBuilder<OffsetRange> = this
        override fun setOpenTelemetryContext(c: io.opentelemetry.context.Context?): OutputBuilder<OffsetRange> = this
        override fun setValueKind(k: org.apache.beam.sdk.values.ValueKind): OutputBuilder<OffsetRange> = this
        override fun output() {}
    }

    private fun fn(): Elasticsearch6ReadFn = Elasticsearch6ReadFn(
        nodes = listOf("http://localhost:9200"),
        username = "",
        password = "",
        schema = DOCUMENT_SCHEMA,
        fields = emptyList(),
        scanQuery = "",
        scrollSize = 1000,
        scrollTimeoutMinutes = 1,
    )

    @Test
    fun `splitRestriction 整段透传且不切分`() {
        val receiver = CollectingReceiver()
        val range = OffsetRange(0, 1000)

        fn().splitRestriction("orders", range, receiver)

        // 该实现不切分：对大区间也只输出 1 段，且正好等于原限制。
        assertEquals(1, receiver.outputs.size)
        assertEquals(range, receiver.outputs[0])
        // 整段覆盖原区间。
        assertEquals(range.from, receiver.outputs[0].from)
        assertEquals(range.to, receiver.outputs[0].to)
    }

    @Test
    fun `空区间 splitRestriction 仍输出原区间（不被丢弃）`() {
        val receiver = CollectingReceiver()
        val range = OffsetRange(5, 5)

        fn().splitRestriction("orders", range, receiver)

        assertEquals(1, receiver.outputs.size)
        assertEquals(range, receiver.outputs[0])
    }

    @Test
    fun `newTracker 暴露的限制与入参一致`() {
        val range = OffsetRange(0, 1)
        val tracker: OffsetRangeTracker = fn().newTracker(range)
        assertEquals(range, tracker.currentRestriction())
    }

    @Test
    fun `restrictionCoder 非 null 且能编码解码`() {
        val coder: Coder<OffsetRange> = fn().restrictionCoder()
        assertNotNull(coder)

        val range = OffsetRange(3, 9)
        val out = java.io.ByteArrayOutputStream()
        coder.encode(range, out)
        val decoded = coder.decode(java.io.ByteArrayInputStream(out.toByteArray()))
        assertEquals(range, decoded)
    }
}
