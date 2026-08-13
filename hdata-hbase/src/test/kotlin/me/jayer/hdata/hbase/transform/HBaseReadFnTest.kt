package me.jayer.hdata.hbase.transform

import me.jayer.hdata.hbase.HBaseRegion
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.testing.TestOutputReceiver
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class HBaseReadFnTest {

    private fun region(): HBaseRegion =
        HBaseRegion("mytable", byteArrayOf(), byteArrayOf(0x7F))

    private fun fn(): HBaseReadFn = HBaseReadFn(
        zookeeperQuorum = "localhost:2181",
        rowkeyField = "rowkey",
        family = "cf",
        schemaFields = listOf("name:STRING", "age:INT32"),
        scanCaching = 100,
    )

    @Test
    fun `getInitialRestriction 返回整段一次读完的区间`() {
        val restriction = fn().getInitialRestriction(region())
        assertEquals(OffsetRange(0, 1), restriction)
    }

    @Test
    fun `splitRestriction 产出恰好一个与输入相等的区间`() {
        val restriction = OffsetRange(0, 1)
        val receiver = TestOutputReceiver<OffsetRange>()
        fn().splitRestriction(region(), restriction, receiver)

        // region 已经是 HBase 天然的并行粒度，Scan 不可在行级被续跑切分，所以不进一步拆分。
        val collected = receiver.outputs
        assertEquals(1, collected.size)
        assertEquals(restriction, collected[0])
        // 覆盖整个区间
        assertEquals(restriction.from, collected[0].from)
        assertEquals(restriction.to, collected[0].to)
    }

    @Test
    fun `splitRestriction 对任意区间都原样透传`() {
        val restriction = OffsetRange(0, 1000)
        val receiver = TestOutputReceiver<OffsetRange>()
        fn().splitRestriction(region(), restriction, receiver)

        val collected = receiver.outputs
        assertEquals(1, collected.size)
        assertEquals(restriction, collected[0])
    }

    @Test
    fun `splitRestriction 对空区间仍产出该空区间`() {
        val restriction = OffsetRange(0, 0)
        val receiver = TestOutputReceiver<OffsetRange>()
        fn().splitRestriction(region(), restriction, receiver)

        val collected = receiver.outputs
        assertEquals(1, collected.size)
        assertEquals(restriction, collected[0])
    }

    @Test
    fun `newTracker 的当前限制等于输入限制`() {
        val restriction = OffsetRange(0, 1)
        val tracker = fn().newTracker(restriction)
        assertEquals(restriction, tracker.currentRestriction())
    }

    @Test
    fun `restrictionCoder 非空`() {
        val coder = fn().restrictionCoder()
        assertNotNull(coder)
        assertTrue(coder.encodedTypeDescriptor.type == OffsetRange::class.java)
    }
}
