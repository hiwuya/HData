package me.jayer.hdata.elasticsearch8

import org.apache.beam.sdk.schemas.Schema
import org.joda.time.Instant as JodaInstant
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.util.Base64
import kotlin.test.assertContentEquals

/**
 * ES 8.x 纯逻辑层此前零覆盖的部分：读端 `convertValue` 与写端 `toJsonValue` 的类型换算、
 * `schema_fields` 解析与 Beam schema 构建、以及类型校验。
 *
 * 这些换算就是"配置项真的生效"的那一环：配了 `schema_fields` 就必须把 `_source` 里的值按声明类型
 * 转成 Beam Row / JSON 能接受的值，转错类型或静默丢转换都只会让数据对不上。
 *
 * @author wuya
 */
class EsSchemaTest {

    @Test
    fun `parseSchemaFields 大小写不敏感`() {
        assertEquals(listOf("id" to "STRING"), parseSchemaFields(listOf("id:string")))
    }

    @Test
    fun `parseSchemaFields 少写冒号报错`() {
        assertThrows(IllegalArgumentException::class.java) { parseSchemaFields(listOf("id")) }
    }

    @Test
    fun `fieldType 不支持的类型报错`() {
        assertThrows(IllegalArgumentException::class.java) { fieldType("UUID") }
    }

    @Test
    fun `buildSchema 空字段退化为 document 列`() {
        val schema = buildSchema(emptyList())
        assertEquals("document", schema.getField(0).name)
    }

    @Test
    fun `buildSchema 按类型建字段`() {
        val schema = buildSchema(listOf("id:INT64", "name:STRING", "amount:DOUBLE", "ok:BOOLEAN", "at:DATETIME", "blob:BYTES", "qty:INT32"))
        assertEquals(Schema.TypeName.INT64, schema.getField("id").type.typeName)
        assertEquals(Schema.TypeName.STRING, schema.getField("name").type.typeName)
        assertEquals(Schema.TypeName.DOUBLE, schema.getField("amount").type.typeName)
        assertEquals(Schema.TypeName.BOOLEAN, schema.getField("ok").type.typeName)
        assertEquals(Schema.TypeName.DATETIME, schema.getField("at").type.typeName)
        assertEquals(Schema.TypeName.BYTES, schema.getField("blob").type.typeName)
        assertEquals(Schema.TypeName.INT32, schema.getField("qty").type.typeName)
    }

    @Test
    fun `convertValue 读端按类型换算`() {
        assertNull(convertValue(null, "STRING"))
        assertEquals("x", convertValue("x", "STRING"))
        assertEquals(7, convertValue(7L, "INT32"))
        assertEquals(7L, convertValue(7, "INT64"))
        assertEquals(1.5, convertValue(1.5, "DOUBLE"))
        assertEquals(true, convertValue("true", "BOOLEAN"))
        assertEquals(JodaInstant.ofEpochMilli(1_700_000_000_000L), convertValue(1_700_000_000_000L, "DATETIME"))
        assertContentEquals(
            byteArrayOf(1, 2, 3),
            convertValue(Base64.getEncoder().encodeToString(byteArrayOf(1, 2, 3)), "BYTES") as ByteArray,
        )
    }

    @Test
    fun `toJsonValue 写端按类型换算`() {
        assertNull(toJsonValue(null, "DATETIME"))
        assertEquals("hi", toJsonValue("hi", "STRING"))
        val at = JodaInstant.ofEpochMilli(1_700_000_000_000L)
        assertEquals(at.toString(), toJsonValue(at, "DATETIME"))
        assertEquals(Base64.getEncoder().encodeToString(byteArrayOf(1, 2, 3)), toJsonValue(byteArrayOf(1, 2, 3), "BYTES"))
    }
}
