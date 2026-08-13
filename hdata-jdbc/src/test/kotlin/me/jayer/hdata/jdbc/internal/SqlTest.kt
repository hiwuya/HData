package me.jayer.hdata.jdbc.internal

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * @author wuya
 * @date 2022-08-30
 */
class SqlTest {

    @Test
    fun `SELECT 拼接列与表`() {
        assertEquals("SELECT a, b FROM t_order", SelectSql("t_order", listOf("a", "b")).render())
    }

    @Test
    fun `默认取全部列`() {
        assertEquals("SELECT * FROM t_order", SelectSql("t_order").render())
    }

    @Test
    fun `空白列被跳过`() {
        assertEquals("SELECT a FROM t_order", SelectSql("t_order", listOf("a", "", "  ")).render())
    }

    @Test
    fun `一列都没有时报错而不是拼出非法 SQL`() {
        assertFailsWith<IllegalArgumentException> { SelectSql("t_order", listOf("", " ")).render() }
    }

    @Test
    fun `多个条件各自加括号后用 AND 连接`() {
        assertEquals(
            "SELECT * FROM t_order WHERE (id > 1) AND (name = 'a')",
            SelectSql("t_order", conditions = listOf("id > 1", "name = 'a'")).render(),
        )
    }

    @Test
    fun `空白条件不产生多余的 WHERE`() {
        assertEquals("SELECT * FROM t_order", SelectSql("t_order", conditions = listOf("", "   ")).render())
    }

    @Test
    fun `withConditions 追加条件且不改动原对象`() {
        val base = SelectSql("t_order", conditions = listOf("id > 1"))
        val appended = base.withConditions("id < ?", "id >= ?")

        assertEquals("SELECT * FROM t_order WHERE (id > 1)", base.render())
        assertEquals(
            "SELECT * FROM t_order WHERE (id > 1) AND (id < ?) AND (id >= ?)",
            appended.render(),
        )
    }

    @Test
    fun `withColumns 替换列且不改动原对象`() {
        val base = SelectSql("t_order", listOf("*"), listOf("id > 1"))
        val aggregated = base.withColumns("min(id)", "max(id)")

        assertEquals(listOf("*"), base.columns)
        assertEquals("SELECT min(id), max(id) FROM t_order WHERE (id > 1)", aggregated.render())
    }

    @Test
    fun `INSERT 的占位符个数与列数一致`() {
        assertEquals(
            "INSERT INTO t_order (a, b, c) VALUES (?, ?, ?)",
            InsertSql.render("t_order", listOf("a", "b", "c")),
        )
    }

    @Test
    fun `INSERT 跳过空白列`() {
        assertEquals(
            "INSERT INTO t_order (a, b) VALUES (?, ?)",
            InsertSql.render("t_order", listOf("a", "", "b")),
        )
    }

    @Test
    fun `INSERT 一列都没有时报错`() {
        assertFailsWith<IllegalArgumentException> { InsertSql.render("t_order", emptyList()) }
    }

    // ---------- 表名 ----------

    @Test
    fun `没有区间语法时原样返回`() {
        assertEquals(listOf("t_order", "t_user"), TableNames.resolve(listOf("t_order", "t_user")))
    }

    @Test
    fun `按区间展开并保留左侧补零宽度`() {
        assertEquals(
            listOf("t_order_00", "t_order_01", "t_order_02"),
            TableNames.resolve(listOf("t_order_\${00-02}")),
        )
    }

    @Test
    fun `不补零时按原样递增`() {
        assertEquals(listOf("t_0", "t_1", "t_2"), TableNames.resolve(listOf("t_\${0-2}")))
    }

    @Test
    fun `起止相同时只展开一张表`() {
        assertEquals(listOf("t_order_07"), TableNames.resolve(listOf("t_order_\${07-07}")))
    }

    @Test
    fun `多个表名各自展开后拼接`() {
        assertEquals(
            listOf("a_0", "a_1", "b", "c_00", "c_01"),
            TableNames.resolve(listOf("a_\${0-1}", "b", "c_\${00-01}")),
        )
    }

    @Test
    fun `区间写反时报错`() {
        val error = assertFailsWith<IllegalArgumentException> { TableNames.resolve(listOf("t_\${5-1}")) }
        assertTrue("from" in error.message!! && "to" in error.message!!)
    }

    @Test
    fun `空列表返回空`() {
        assertEquals(emptyList(), TableNames.resolve(emptyList()))
    }

    @Test
    fun `拆分限定表名`() {
        assertEquals(null to "t_order", TableNames.split("t_order"))
        assertEquals("db" to "t_order", TableNames.split("db.t_order"))
        assertEquals("db" to "t_order", TableNames.split("`db`.`t_order`"))
        assertEquals("db.schema" to "t_order", TableNames.split("db.schema.t_order"))
    }
}
