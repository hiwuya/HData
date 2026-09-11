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
    fun `SELECT concatenates columns and table`() {
        assertEquals("SELECT a, b FROM t_order", SelectSql("t_order", listOf("a", "b")).render())
    }

    @Test
    fun `defaults to selecting all columns`() {
        assertEquals("SELECT * FROM t_order", SelectSql("t_order").render())
    }

    @Test
    fun `blank columns are skipped`() {
        assertEquals("SELECT a FROM t_order", SelectSql("t_order", listOf("a", "", "  ")).render())
    }

    @Test
    fun `errors instead of building invalid SQL when there are no columns at all`() {
        assertFailsWith<IllegalArgumentException> { SelectSql("t_order", listOf("", " ")).render() }
    }

    @Test
    fun `multiple conditions are each parenthesized and joined with AND`() {
        assertEquals(
            "SELECT * FROM t_order WHERE (id > 1) AND (name = 'a')",
            SelectSql("t_order", conditions = listOf("id > 1", "name = 'a'")).render(),
        )
    }

    @Test
    fun `blank conditions don't produce a stray WHERE`() {
        assertEquals("SELECT * FROM t_order", SelectSql("t_order", conditions = listOf("", "   ")).render())
    }

    @Test
    fun `withConditions appends conditions without mutating the original object`() {
        val base = SelectSql("t_order", conditions = listOf("id > 1"))
        val appended = base.withConditions("id < ?", "id >= ?")

        assertEquals("SELECT * FROM t_order WHERE (id > 1)", base.render())
        assertEquals(
            "SELECT * FROM t_order WHERE (id > 1) AND (id < ?) AND (id >= ?)",
            appended.render(),
        )
    }

    @Test
    fun `withColumns replaces columns without mutating the original object`() {
        val base = SelectSql("t_order", listOf("*"), listOf("id > 1"))
        val aggregated = base.withColumns("min(id)", "max(id)")

        assertEquals(listOf("*"), base.columns)
        assertEquals("SELECT min(id), max(id) FROM t_order WHERE (id > 1)", aggregated.render())
    }

    @Test
    fun `INSERT's placeholder count matches the column count`() {
        assertEquals(
            "INSERT INTO t_order (a, b, c) VALUES (?, ?, ?)",
            InsertSql.render("t_order", listOf("a", "b", "c")),
        )
    }

    @Test
    fun `INSERT skips blank columns`() {
        assertEquals(
            "INSERT INTO t_order (a, b) VALUES (?, ?)",
            InsertSql.render("t_order", listOf("a", "", "b")),
        )
    }

    @Test
    fun `INSERT errors when there are no columns at all`() {
        assertFailsWith<IllegalArgumentException> { InsertSql.render("t_order", emptyList()) }
    }

    // ---------- table names ----------

    @Test
    fun `returns as-is when there's no range syntax`() {
        assertEquals(listOf("t_order", "t_user"), TableNames.resolve(listOf("t_order", "t_user")))
    }

    @Test
    fun `expands by range and keeps the left-padded zero width`() {
        assertEquals(
            listOf("t_order_00", "t_order_01", "t_order_02"),
            TableNames.resolve(listOf("t_order_\${00-02}")),
        )
    }

    @Test
    fun `increments as-is when there's no zero-padding`() {
        assertEquals(listOf("t_0", "t_1", "t_2"), TableNames.resolve(listOf("t_\${0-2}")))
    }

    @Test
    fun `expands to just one table when start equals end`() {
        assertEquals(listOf("t_order_07"), TableNames.resolve(listOf("t_order_\${07-07}")))
    }

    @Test
    fun `multiple table names are each expanded then concatenated`() {
        assertEquals(
            listOf("a_0", "a_1", "b", "c_00", "c_01"),
            TableNames.resolve(listOf("a_\${0-1}", "b", "c_\${00-01}")),
        )
    }

    @Test
    fun `errors when the range is written backwards`() {
        val error = assertFailsWith<IllegalArgumentException> { TableNames.resolve(listOf("t_\${5-1}")) }
        assertTrue("from" in error.message!! && "to" in error.message!!)
    }

    @Test
    fun `an oversized range errors before expansion instead of exhausting memory`() {
        val error = assertFailsWith<IllegalArgumentException> {
            TableNames.resolve(listOf("t_\${0-999999999999999999999999}"))
        }
        assertTrue(TableNames.MAX_RESOLVED_TABLES.toString() in error.message!!)
    }

    @Test
    fun `multiple range segments are not ambiguously substituted in sync`() {
        val error = assertFailsWith<IllegalArgumentException> {
            TableNames.resolve(listOf("t_\${0-1}_\${00-01}"))
        }
        assertTrue("at most one" in error.message!!)
    }

    @Test
    fun `an empty list returns empty`() {
        assertEquals(emptyList(), TableNames.resolve(emptyList()))
    }

    @Test
    fun `splits a qualified table name`() {
        assertEquals(null to "t_order", TableNames.split("t_order"))
        assertEquals("db" to "t_order", TableNames.split("db.t_order"))
        assertEquals("db" to "t_order", TableNames.split("`db`.`t_order`"))
        assertEquals("db.schema" to "t_order", TableNames.split("db.schema.t_order"))
    }
}
