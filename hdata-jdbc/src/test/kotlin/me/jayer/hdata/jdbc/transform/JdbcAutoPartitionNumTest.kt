package me.jayer.hdata.jdbc.transform

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 守护 JDBC 自动分区估算：跨度极大时也必须夹在 [JdbcPartitionedReadFn.MAX_PARTITIONS] 以内，
 * 否则一个作业会向单个 RDBMS 发起海量并行查询把它打挂。
 */
class JdbcAutoPartitionNumTest {

    @Test
    fun `巨型跨度仍被夹在上限内`() {
        val num = JdbcPartitionedReadFn.autoPartitionNum(1_000_000_000_000L, "t")
        assertTrue(num <= JdbcPartitionedReadFn.MAX_PARTITIONS, "自动分区数 $num 超过上限")
        assertEquals(JdbcPartitionedReadFn.MAX_PARTITIONS, num)
    }

    @Test
    fun `小跨度按开方估算且不爆下限`() {
        val num = JdbcPartitionedReadFn.autoPartitionNum(10_000L, "t")
        assertTrue(num in 1..JdbcPartitionedReadFn.MAX_PARTITIONS)
    }
}
