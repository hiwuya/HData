package me.jayer.hdata.filesystem

import org.apache.beam.sdk.schemas.Schema
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * `RecordParser` 把字符串字段按 schema 转成 [Row]。
 *
 * 解析失败报错必须带上**文件名与行号**——重构前 `raw.toInt()` 抛的 `NumberFormatException`
 * 只说得出 `For input string: "abc"`，几百万行的 CSV 里无从查起。
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
    fun `字段按声明类型解析，空字符串视为 null`() {
        val row = parser.parse(listOf("30", "张三", "true", "100"), "f.csv", 1)
        assertEquals(30, row.getInt32("age"))
        assertEquals("张三", row.getString("name"))
        assertEquals(true, row.getBoolean("ok"))
        assertEquals(100L, row.getInt64("big"))
    }

    @Test
    fun `空白字段解析为 null 而不是抛异常`() {
        val row = parser.parse(listOf("", "", "", ""), "f.csv", 2)
        assertNull(row.getValue("age"))
        assertNull(row.getValue("name"))
    }

    @Test
    fun `布尔值只认明确写法，其余一律报错`() {
        // 只认 true/false/1/0/yes/no/y/n/t/f；"maybe" 这种必须拒绝，而不是静默当成 false
        val e = assertFailsWith<IllegalArgumentException> { parser.parse(listOf("1", "x", "maybe", "1"), "f.csv", 3) }
        assertTrue("ok" in e.message!!)
    }

    @Test
    fun `解析错误带文件名 行号 列名`() {
        val e = assertFailsWith<IllegalArgumentException> { parser.parse(listOf("abc", "x", "true", "1"), "orders.csv", 7) }
        val msg = e.message!!
        assertTrue("orders.csv" in msg, "应带文件名: $msg")
        assertTrue("第 7 行" in msg, "应带行号: $msg")
        assertTrue("age" in msg, "应带列名: $msg")
    }

    @Test
    fun `多余列会拒绝而不是静默丢弃`() {
        val error = assertFailsWith<IllegalArgumentException> {
            parser.parse(listOf("1", "x", "true", "2", "lost"), "orders.csv", 9)
        }
        assertTrue("超过" in error.message!! && "orders.csv" in error.message!!)
    }
}
