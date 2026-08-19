package me.jayer.hdata.ftp

import org.apache.beam.sdk.schemas.Schema
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * `schema_fields` 解析的纯逻辑边界。
 *
 * `parseSchemaField` 早先是 `else -> STRING`：把 `age:intt` 这种拼写错误静默当成 STRING，
 * 一直到下游对不上号才发作。这些分支必须显式报错，且真的拦得住。
 *
 * @author wuya
 */
class FtpSchemaFieldsTest {

    @Test
    fun `解析出字段名与类型`() {
        val (name, type) = parseSchemaField("age:int")
        assertEquals("age", name)
        assertEquals(Schema.TypeName.INT32, type.typeName)
    }

    @Test
    fun `类型别名都认得`() {
        assertEquals(Schema.FieldType.INT64, parseSchemaField("id:long").second)
        assertEquals(Schema.FieldType.INT64, parseSchemaField("id:int64").second)
        assertEquals(Schema.FieldType.FLOAT, parseSchemaField("f:float").second)
        assertEquals(Schema.FieldType.DOUBLE, parseSchemaField("d:double").second)
        assertEquals(Schema.FieldType.BOOLEAN, parseSchemaField("b:boolean").second)
        assertEquals(Schema.FieldType.BOOLEAN, parseSchemaField("b:bool").second)
        assertEquals(Schema.FieldType.STRING, parseSchemaField("s:string").second)
    }

    @Test
    fun `不带类型时默认 string`() {
        assertEquals(Schema.FieldType.STRING, parseSchemaField("note").second)
    }

    @Test
    fun `字段名不能为空`() {
        val e = assertFailsWith<IllegalArgumentException> { parseSchemaField(":int") }
        assertTrue("不能为空" in e.message!!)
    }

    @Test
    fun `类型不认识时报错并列出可选值`() {
        val e = assertFailsWith<IllegalArgumentException> { parseSchemaField("x:intt") }
        assertTrue("intt" in e.message!!)
        assertTrue("string" in e.message!!)
    }

    @Test
    fun `csv schema 由 schema_fields 构建`() {
        val schema = buildReadSchema(
            FtpReadConfig(host = "h", path = "/in", fileFormat = "csv", schemaFields = listOf("name:string", "age:int")),
        )
        assertEquals(Schema.TypeName.STRING, schema.getField("name").type.typeName)
        assertEquals(Schema.TypeName.INT32, schema.getField("age").type.typeName)
    }
}
