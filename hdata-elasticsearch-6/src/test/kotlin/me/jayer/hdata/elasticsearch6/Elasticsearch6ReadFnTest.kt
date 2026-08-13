package me.jayer.hdata.elasticsearch6

import me.jayer.hdata.core.testing.CollectingOutputReceiver
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.util.SerializableUtils
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * ES 6.x 读取端的切分与可序列化。
 *
 * @author wuya
 */
class Elasticsearch6ReadFnTest {

    private val config = Elasticsearch6ReadConfig(connectionUri = "http://localhost:9200", index = "orders")

    private fun fn(slices: Int = 1) = Elasticsearch6ReadFn(
        config.nodes(),
        config.username,
        config.password,
        DOCUMENT_SCHEMA,
        emptyList(),
        config.scanQuery,
        config.scrollSize,
        config.scrollTimeoutMinutes,
        slices,
    )

    @Test
    fun `DoFn 可以序列化下发`() {
        SerializableUtils.ensureSerializable(fn(4))
    }

    @Test
    fun `初始限制覆盖全部 slice`() {
        assertEquals(OffsetRange(0, 4), fn(4).getInitialRestriction("orders"))
        assertEquals(OffsetRange(0, 1), fn().getInitialRestriction("orders"))
    }

    @Test
    fun `每个 slice 切成一份，首尾相接`() {
        val receiver = CollectingOutputReceiver<OffsetRange>()
        fn(3).splitRestriction("orders", OffsetRange(0, 3), receiver)
        val splits = receiver.outputs

        assertEquals(listOf(0L, 1L, 2L), splits.map { it.from })
        assertEquals(listOf(1L, 2L, 3L), splits.map { it.to })
    }

    @Test
    fun `空区间不产出切分`() {
        val receiver = CollectingOutputReceiver<OffsetRange>()
        fn().splitRestriction("orders", OffsetRange(0, 0), receiver)
        val splits = receiver.outputs

        assertTrue(splits.isEmpty())
    }

    @Test
    fun `scan_slices 非法时报错`() {
        assertFailsWith<IllegalArgumentException> { config.copy(scanSlices = 0).validate() }
        assertFailsWith<IllegalArgumentException> { config.copy(scrollTimeoutMinutes = 0).validate() }
    }
}
