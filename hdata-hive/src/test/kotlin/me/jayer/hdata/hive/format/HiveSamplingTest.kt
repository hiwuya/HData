package me.jayer.hdata.hive.format

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class HiveSamplingTest {

    @Test
    fun `block sampling result is stable given the same seed, file, and offset`() {
        val value = sampleBlock(42, "file:///warehouse/t/data.orc", 4096)

        assertEquals(value, sampleBlock(42, "file:///warehouse/t/data.orc", 4096))
        assertNotEquals(value, sampleBlock(42, "file:///warehouse/t/data.orc", 8192))
        assertNotEquals(value, sampleBlock(42, "file:///warehouse/t/other.orc", 4096))
        assertTrue(value in 0.0..<1.0)
    }

    @Test
    fun `different blocks do not reuse the same first value of the random sequence`() {
        val decisions = (0L until 64L).map { sampleBlock(7, "file:///data.parquet", it * 4096) < 0.5 }

        assertTrue(decisions.any { it })
        assertTrue(decisions.any { !it })
    }
}
