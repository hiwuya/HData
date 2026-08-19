package me.jayer.hdata.elasticsearch6

import org.apache.beam.sdk.schemas.Schema
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.util.Base64

/**
 * ES 6.x 纯逻辑层此前零覆盖的部分：`schema_fields` 解析、Beam schema 构建、以及写端
 * `esValue` 的字段类型换算（DATETIME→epochMillis、BYTES→base64 等）。
 *
 * `esValue` 就是"配置项真的生效"的那一环：配了 `schema_fields` 就必须按类型把行值转成
 * ES 能序列化的 JSON 友好值，写错类型或静默丢转换都只会让数据对不上。
 *
 * @author wuya
 */
class Elasticsearch6SchemaTest {

    @Test
    fun `parseSchemaFields 类型大小写不敏感`() {
        assertEquals(EsField("id", EsFieldType.INT64), parseSchemaFields(listOf("id:int64")).single())
    }

    @Test
    fun `parseSchemaFields 不支持的类型报错`() {
        assertThrows(IllegalArgumentException::class.java) { parseSchemaFields(listOf("id:UUID")) }
    }

    @Test
    fun `parseSchemaFields 少写冒号报错`() {
        assertThrows(IllegalArgumentException::class.java) { parseSchemaFields(listOf("id")) }
    }

    @Test
    fun `没给 schema_fields 时读端默认是单 document 列`() {
        assertEquals("document", DOCUMENT_SCHEMA.getField(0).name)
        assertEquals(Schema.TypeName.STRING, DOCUMENT_SCHEMA.getField("document").type.typeName)
    }

    @Test
    fun `buildSchema 按类型建字段`() {
        val schema = buildSchema(
            parseSchemaFields(
                listOf("id:INT64", "name:STRING", "amount:DOUBLE", "ok:BOOLEAN", "at:DATETIME", "blob:BYTES", "qty:INT32")
            )
        )
        assertEquals(Schema.TypeName.INT64, schema.getField("id").type.typeName)
        assertEquals(Schema.TypeName.STRING, schema.getField("name").type.typeName)
        assertEquals(Schema.TypeName.DOUBLE, schema.getField("amount").type.typeName)
        assertEquals(Schema.TypeName.BOOLEAN, schema.getField("ok").type.typeName)
        assertEquals(Schema.TypeName.DATETIME, schema.getField("at").type.typeName)
        assertEquals(Schema.TypeName.BYTES, schema.getField("blob").type.typeName)
        assertEquals(Schema.TypeName.INT32, schema.getField("qty").type.typeName)
    }

    @Test
    fun `esValue 把各类型转成 JSON 友好值`() {
        assertEquals("x", esValue(EsFieldType.STRING, "x"))
        assertEquals(7, esValue(EsFieldType.INT32, 7L))
        assertEquals(7L, esValue(EsFieldType.INT64, 7))
        assertEquals(1.5, esValue(EsFieldType.DOUBLE, 1.5))
        assertEquals(true, esValue(EsFieldType.BOOLEAN, true))
        assertEquals(
            1_700_000_000_000L,
            esValue(EsFieldType.DATETIME, org.joda.time.Instant.ofEpochMilli(1_700_000_000_000L)),
        )
        assertEquals(
            Base64.getEncoder().encodeToString(byteArrayOf(1, 2, 3)),
            esValue(EsFieldType.BYTES, byteArrayOf(1, 2, 3)),
        )
        assertNull(esValue(EsFieldType.STRING, null))
    }

    @Test
    fun `非法值拒绝而不是截断或静默变成 false null`() {
        assertThrows(IllegalArgumentException::class.java) { esValue(EsFieldType.INT32, 1.5) }
        assertThrows(IllegalArgumentException::class.java) { esValue(EsFieldType.INT32, 2_147_483_648L) }
        assertThrows(IllegalArgumentException::class.java) { esValue(EsFieldType.BOOLEAN, "maybe") }
        assertThrows(IllegalArgumentException::class.java) { esValue(EsFieldType.BYTES, "not-base64!") }
        assertThrows(IllegalArgumentException::class.java) { esRowValue(EsFieldType.BYTES, "not-base64!") }
    }
}
