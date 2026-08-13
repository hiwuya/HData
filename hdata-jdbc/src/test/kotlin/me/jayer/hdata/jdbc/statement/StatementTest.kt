package me.jayer.hdata.jdbc.statement

import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * @author wuya
 * @date 2022-08-30
 */
class StatementTest {

    @Test
    fun `SELECT 拼接列与表`() {
        assertEquals(
            "SELECT a, b FROM t_order",
            SelectStatement(listOf("a", "b"), "t_order").buildSql(),
        )
    }

    @Test
    fun `空白列被跳过`() {
        assertEquals(
            "SELECT a FROM t_order",
            SelectStatement(listOf("a", "", "  "), "t_order").buildSql(),
        )
    }

    @Test
    fun `多个 where 条件各自加括号后用 AND 连接`() {
        assertEquals(
            "SELECT * FROM t_order WHERE (id > 1) AND (name = 'a')",
            SelectStatement(listOf("*"), "t_order", listOf("id > 1", "name = 'a'")).buildSql(),
        )
    }

    @Test
    fun `空白 where 条件不产生多余的 WHERE`() {
        assertEquals(
            "SELECT * FROM t_order",
            SelectStatement(listOf("*"), "t_order", listOf("", "   ")).buildSql(),
        )
    }

    @Test
    fun `appendWhere 追加条件且不改动原对象`() {
        val base = SelectStatement(listOf("*"), "t_order", listOf("id > 1"))
        val appended = base.appendWhere("id < ?", "id >= ?")

        assertEquals("SELECT * FROM t_order WHERE (id > 1)", base.buildSql())
        assertEquals(
            "SELECT * FROM t_order WHERE (id > 1) AND (id < ?) AND (id >= ?)",
            appended.buildSql(),
        )
    }

    @Test
    fun `columns 替换列且不改动原对象`() {
        val base = SelectStatement(listOf("*"), "t_order", listOf("id > 1"))
        val aggregated = base.columns("min(id)", "max(id)")

        assertEquals(listOf("*"), base.columns)
        assertEquals(
            "SELECT min(id), max(id) FROM t_order WHERE (id > 1)",
            aggregated.buildSql(),
        )
    }

    @Test
    fun `INSERT 的占位符个数与列数一致`() {
        assertEquals(
            "INSERT INTO t_order (a, b, c) VALUES (?, ?, ?)",
            InsertStatement(listOf("a", "b", "c"), "t_order").buildSql(),
        )
    }

    @Test
    fun `INSERT 跳过空白列`() {
        assertEquals(
            "INSERT INTO t_order (a, b) VALUES (?, ?)",
            InsertStatement(listOf("a", "", "b"), "t_order").buildSql(),
        )
    }
}
