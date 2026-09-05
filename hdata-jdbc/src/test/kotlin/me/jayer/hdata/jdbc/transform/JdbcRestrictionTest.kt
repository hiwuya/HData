package me.jayer.hdata.jdbc.transform

import org.apache.beam.sdk.io.range.OffsetRange
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class JdbcRestrictionTest {

    @Test
    fun `查询块完整覆盖不整除的数据区间`() {
        val restriction = JdbcRestriction(0, 10, 0, 4, 4, 2)

        assertEquals(
            listOf(OffsetRange(0, 3), OffsetRange(3, 6), OffsetRange(6, 8), OffsetRange(8, 10)),
            (0L until 4L).map(restriction::dataRange),
        )
    }

    @Test
    fun `查询块支持跨过零点而不发生算术溢出`() {
        val restriction = JdbcRestriction(-10, 90, 0, 4, 4, 2)

        assertEquals(
            listOf(OffsetRange(-10, 15), OffsetRange(15, 40), OffsetRange(40, 65), OffsetRange(65, 90)),
            (0L until 4L).map(restriction::dataRange),
        )
    }

    @Test
    fun `运行时切分只发生在 SQL 查询块边界且不重不漏`() {
        val restriction = JdbcRestriction(0, 100, 0, 4, 4, 1)
        val tracker = JdbcRestrictionTracker(restriction)
        assertTrue(tracker.tryClaim(0))

        val split = assertNotNull(tracker.trySplit(0.5))
        val primary = assertNotNull(split.primary)
        val residual = assertNotNull(split.residual)
        val parts: List<JdbcRestriction> = listOf(primary, residual)
        val ranges = parts.flatMap { part ->
            (part.chunkFrom until part.chunkTo).map(part::dataRange)
        }.sortedBy { it.from }

        assertEquals(0, ranges.first().from)
        assertEquals(100, ranges.last().to)
        ranges.zipWithNext().forEach { (left, right) -> assertEquals(left.to, right.from) }
        assertTrue(residual.chunkFrom > 0)
    }

    @Test
    fun `NULL 查询块不参与数值边界均分`() {
        val restriction = JdbcRestriction(1, 21, 0, 5, 5, 2, hasNulls = true)

        assertEquals(4, restriction.numericChunkCount)
        assertEquals(
            listOf(OffsetRange(1, 6), OffsetRange(6, 11), OffsetRange(11, 16), OffsetRange(16, 21)),
            (0L until restriction.numericChunkCount).map(restriction::dataRange),
        )
        assertFailsWith<IllegalArgumentException> { restriction.dataRange(4) }
    }

    @Test
    fun `全 NULL 分区列只有 NULL 查询块`() {
        val restriction = JdbcRestriction(0, 0, 0, 1, 1, 1, hasNulls = true)

        assertEquals(0, restriction.numericChunkCount)
        assertEquals(OffsetRange(0, 1), restriction.chunkRange())
        assertFailsWith<IllegalArgumentException> { restriction.dataRange(0) }
    }
}
