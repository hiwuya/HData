package me.jayer.hdata.jdbc.transform

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Guards the JDBC auto partition estimate: even an extreme span must stay clamped within [JdbcPartitionedReadFn.MAX_PARTITIONS],
 * otherwise one job would open a flood of parallel queries against a single RDBMS and take it down.
 */
class JdbcAutoPartitionNumTest {

    @Test
    fun `巨型跨度仍被夹在上限内`() {
        val num = JdbcPartitionedReadFn.autoPartitionNum(1_000_000_000_000L, "t")
        assertTrue(num <= JdbcPartitionedReadFn.MAX_PARTITIONS, "auto partition count $num exceeds the upper bound")
        assertEquals(JdbcPartitionedReadFn.MAX_PARTITIONS, num)
    }

    @Test
    fun `小跨度按开方估算且不爆下限`() {
        val num = JdbcPartitionedReadFn.autoPartitionNum(10_000L, "t")
        assertTrue(num in 1..JdbcPartitionedReadFn.MAX_PARTITIONS)
    }
}
