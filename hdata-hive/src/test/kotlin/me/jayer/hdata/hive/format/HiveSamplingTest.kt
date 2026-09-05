package me.jayer.hdata.hive.format

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class HiveSamplingTest {

    @Test
    fun `块采样结果由种子文件和偏移稳定决定`() {
        val value = sampleBlock(42, "file:///warehouse/t/data.orc", 4096)

        assertEquals(value, sampleBlock(42, "file:///warehouse/t/data.orc", 4096))
        assertNotEquals(value, sampleBlock(42, "file:///warehouse/t/data.orc", 8192))
        assertNotEquals(value, sampleBlock(42, "file:///warehouse/t/other.orc", 4096))
        assertTrue(value in 0.0..<1.0)
    }

    @Test
    fun `不同块不会重复使用随机序列的同一个首值`() {
        val decisions = (0L until 64L).map { sampleBlock(7, "file:///data.parquet", it * 4096) < 0.5 }

        assertTrue(decisions.any { it })
        assertTrue(decisions.any { !it })
    }
}
