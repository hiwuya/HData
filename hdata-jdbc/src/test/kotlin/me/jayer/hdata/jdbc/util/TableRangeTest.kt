package me.jayer.hdata.jdbc.util

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * 分表名区间语法 `t_${'$'}{00-15}` 的展开。
 *
 * @author wuya
 * @date 2022-08-30
 */
class TableRangeTest {

    @Test
    fun `没有区间语法时原样返回`() {
        assertEquals(listOf("t_order", "t_user"), JdbcUtils.resolveTables(listOf("t_order", "t_user")))
    }

    @Test
    fun `按区间展开并保留左侧补零宽度`() {
        assertEquals(
            listOf("t_order_00", "t_order_01", "t_order_02"),
            JdbcUtils.resolveTables(listOf("t_order_\${00-02}")),
        )
    }

    @Test
    fun `不补零时按原样递增`() {
        assertEquals(
            listOf("t_order_0", "t_order_1", "t_order_2"),
            JdbcUtils.resolveTables(listOf("t_order_\${0-2}")),
        )
    }

    @Test
    fun `起止相同时只展开一张表`() {
        assertEquals(listOf("t_order_07"), JdbcUtils.resolveTables(listOf("t_order_\${07-07}")))
    }

    @Test
    fun `多个表名各自展开后拼接`() {
        assertEquals(
            listOf("a_0", "a_1", "b", "c_00", "c_01"),
            JdbcUtils.resolveTables(listOf("a_\${0-1}", "b", "c_\${00-01}")),
        )
    }

    @Test
    fun `区间写反时报错`() {
        val error = assertFailsWith<IllegalArgumentException> {
            JdbcUtils.resolveTables(listOf("t_order_\${5-1}"))
        }
        assertTrue("from" in error.message!! && "to" in error.message!!)
    }

    @Test
    fun `空列表返回空`() {
        assertEquals(emptyList(), JdbcUtils.resolveTables(emptyList()))
    }
}
