package me.jayer.hdata.filesystem

import org.apache.beam.sdk.schemas.Schema
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * `schema_fields` 解析的纯逻辑边界。
 *
 * 类型不认识时**直接报错**——重构前这里是 `else -> STRING`，把 `age:intt` 这种拼写错误
 * 静默当成 STRING，一直到下游对不上号才发作。
 *
 * @author wuya
 */
class FilesystemSchemasTest {

    @Test
    fun `所有支持的类型都能解析`() {
        val schema = FilesystemSchemas.build(
            listOf(
                "s:string", "i:int", "i2:integer", "l:long", "f:float", "d:double",
                "b:boolean", "sh:short", "by:byte",
            ),
        )
        assertEquals(Schema.TypeName.STRING, schema.getField("s").type.typeName)
        assertEquals(Schema.TypeName.INT32, schema.getField("i").type.typeName)
        assertEquals(Schema.TypeName.INT32, schema.getField("i2").type.typeName)
        assertEquals(Schema.TypeName.INT64, schema.getField("l").type.typeName)
        assertEquals(Schema.TypeName.FLOAT, schema.getField("f").type.typeName)
        assertEquals(Schema.TypeName.DOUBLE, schema.getField("d").type.typeName)
        assertEquals(Schema.TypeName.BOOLEAN, schema.getField("b").type.typeName)
        assertEquals(Schema.TypeName.INT16, schema.getField("sh").type.typeName)
        assertEquals(Schema.TypeName.BYTE, schema.getField("by").type.typeName)
    }

    @Test
    fun `缺少冒号的条目报错`() {
        val e = assertFailsWith<IllegalArgumentException> { FilesystemSchemas.build(listOf("name")) }
        assertTrue("name:type" in e.message!!)
    }

    @Test
    fun `字段名为空报错`() {
        val e = assertFailsWith<IllegalArgumentException> { FilesystemSchemas.build(listOf(":string")) }
        assertTrue("不能为空" in e.message!!)
    }

    @Test
    fun `不支持的类型报错并列出可选值`() {
        val e = assertFailsWith<IllegalArgumentException> { FilesystemSchemas.build(listOf("x:intt")) }
        assertTrue("intt" in e.message!!)
        assertTrue("string" in e.message!!)
    }

    @Test
    fun `空 schema_fields 用单行 text schema`() {
        assertEquals(FilesystemSchemas.TEXT_SCHEMA, FilesystemSchemas.build(emptyList()))
    }
}
