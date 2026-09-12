package me.jayer.hdata.filesystem

import org.apache.beam.sdk.schemas.Schema
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * `RecordParser` converts string fields into a [Row] according to a schema.
 *
 * A parse failure must carry the **file name and line number** -- before the refactor the
 * `NumberFormatException` thrown by `raw.toInt()` could only say `For input string: "abc"`, which is
 * impossible to trace in a CSV with millions of rows.
 *
 * @author wuya
 */
class RecordParserTest {

    private val schema: Schema = Schema.builder()
        .addNullableInt32Field("age")
        .addNullableStringField("name")
        .addNullableBooleanField("ok")
        .addNullableField("big", Schema.FieldType.INT64)
        .build()

    private val parser = RecordParser(schema)

    @Test
    fun `fields are parsed by the declared type and empty strings become null`() {
        val row = parser.parse(listOf("30", "Alice", "true", "100"), "f.csv", 1)
        assertEquals(30, row.getInt32("age"))
        assertEquals("Alice", row.getString("name"))
        assertEquals(true, row.getBoolean("ok"))
        assertEquals(100L, row.getInt64("big"))
    }

    @Test
    fun `blank fields parse to null instead of throwing`() {
        val row = parser.parse(listOf("", "", "", ""), "f.csv", 2)
        assertNull(row.getValue("age"))
        assertNull(row.getValue("name"))
    }

    @Test
    fun `booleans only accept explicit forms and reject everything else`() {
        // only true/false/1/0/yes/no/y/n/t/f are accepted; something like "maybe" must be rejected instead of being silently treated as false
        val e = assertFailsWith<IllegalArgumentException> { parser.parse(listOf("1", "x", "maybe", "1"), "f.csv", 3) }
        assertTrue("ok" in e.message!!)
    }

    @Test
    fun `parse errors carry the file name line number and column name`() {
        val e = assertFailsWith<IllegalArgumentException> { parser.parse(listOf("abc", "x", "true", "1"), "orders.csv", 7) }
        val msg = e.message!!
        assertTrue("orders.csv" in msg, "should carry the file name: $msg")
        assertTrue("line 7" in msg, "should carry the line number: $msg")
        assertTrue("age" in msg, "should carry the column name: $msg")
    }

    @Test
    fun `extra columns are rejected instead of being silently dropped`() {
        val error = assertFailsWith<IllegalArgumentException> {
            parser.parse(listOf("1", "x", "true", "2", "lost"), "orders.csv", 9)
        }
        assertTrue("exceeding" in error.message!! && "orders.csv" in error.message!!)
    }
}
