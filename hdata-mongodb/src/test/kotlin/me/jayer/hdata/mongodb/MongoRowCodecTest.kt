package me.jayer.hdata.mongodb

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.util.SerializableUtils
import org.apache.beam.sdk.values.Row
import org.bson.Document
import org.bson.types.Binary
import java.util.Date
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Bson Document 与 Beam Row 的互转。
 *
 * @author wuya
 */
class MongoRowCodecTest {

    private val codec = MongoRowCodec.of(
        listOf("id:STRING", "amount:DOUBLE", "qty:INT32", "big:INT64", "ok:BOOLEAN", "at:DATETIME", "blob:BYTES")
    )

    @Test
    fun `文档读成行，缺失字段为 null`() {
        val row = codec.toRow(Document("id", "a1").append("amount", 12.5))

        assertEquals("a1", row.getString("id"))
        assertEquals(12.5, row.getDouble("amount"))
        assertNull(row.getInt32("qty"))
    }

    @Test
    fun `数值类型之间宽松转换`() {
        // MongoDB 无 schema，同一个字段在不同文档里存成 Int 还是 Long 全看写入方；
        // 重构前用 doc.getInteger(name)，遇到 Long 直接 ClassCastException 把整个作业干掉
        val row = codec.toRow(Document("qty", 7L).append("big", 3).append("amount", 5))

        assertEquals(7, row.getInt32("qty"))
        assertEquals(3L, row.getInt64("big"))
        assertEquals(5.0, row.getDouble("amount"))
    }

    @Test
    fun `类型完全对不上时报错并指出是哪个字段`() {
        val error = assertFailsWith<IllegalArgumentException> {
            codec.toRow(Document("qty", "七"))
        }

        assertTrue("qty" in error.message!! && "INT32" in error.message!!)
    }

    @Test
    fun `整数小数和越界值拒绝而不是截断回绕`() {
        assertFailsWith<IllegalArgumentException> { codec.toRow(Document("qty", 1.5)) }
        assertFailsWith<IllegalArgumentException> { codec.toRow(Document("qty", 2_147_483_648L)) }
    }

    @Test
    fun `行与文档双向往返，值保持一致`() {
        val at = Date(1_700_000_000_000L)
        val bytes = byteArrayOf(1, 2, 3)
        val original = Document("id", "a1")
            .append("amount", 12.5)
            .append("qty", 7)
            .append("big", 9_000_000_000L)
            .append("ok", true)
            .append("at", at)
            .append("blob", Binary(bytes))

        val row = codec.toRow(original)
        val back = codec.toDocument(row)

        assertEquals("a1", back["id"])
        assertEquals(12.5, back["amount"])
        assertEquals(7, back["qty"])
        assertEquals(9_000_000_000L, back["big"])
        assertEquals(true, back["ok"])
        assertEquals(at, back["at"])
        assertContentEquals(bytes, (back["blob"] as Binary).data)
    }

    @Test
    fun `不配 schema_fields 时读写用的是同一个列名`() {
        // 重构前读端产出 document 列、写端却去找 value 列，
        // ReadFromMongoDb 的输出直接接 WriteToMongoDb 会报"缺少字段"
        val plain = MongoRowCodec.of(emptyList())

        val row = plain.toRow(Document("a", 1).append("b", "x"))
        val back = plain.toDocument(row)

        assertEquals(MongoRowCodec.DOCUMENT_FIELD, plain.schema.getField(0).name)
        assertEquals(1, back["a"])
        assertEquals("x", back["b"])
    }

    @Test
    fun `document 模式下缺列时的报错点名正确的字段`() {
        val plain = MongoRowCodec.of(emptyList())
        val row = Row.withSchema(Schema.builder().addNullableStringField("value").build()).addValue("{}").build()

        val error = assertFailsWith<IllegalArgumentException> { plain.toDocument(row) }

        assertTrue(MongoRowCodec.DOCUMENT_FIELD in error.message!!)
    }

    @Test
    fun `写入行缺少声明过的字段时报错`() {
        val row = Row.withSchema(Schema.builder().addNullableStringField("id").build()).addValue("a1").build()

        val error = assertFailsWith<IllegalArgumentException> { codec.toDocument(row) }

        assertTrue("amount" in error.message!!)
    }

    @Test
    fun `投影只请求声明过的字段`() {
        val projection = MongoRowCodec.of(listOf("id:STRING", "amount:DOUBLE")).projection()

        assertEquals(setOf("id", "amount"), projection!!.keys)
        // document 模式要整个文档，不能带投影
        assertNull(MongoRowCodec.of(emptyList()).projection())
    }

    @Test
    fun `codec 可以跟着 DoFn 一起序列化下发`() {
        SerializableUtils.ensureSerializable(codec)
    }

    @Test
    fun `schema_fields 条目格式不对时报错`() {
        assertFailsWith<IllegalArgumentException> { parseSchemaFields(listOf("id")) }
        assertFailsWith<IllegalArgumentException> { parseSchemaFields(listOf(":STRING")) }
        assertFailsWith<IllegalArgumentException> { parseSchemaFields(listOf("id:UUID")) }
        assertFailsWith<IllegalArgumentException> { parseSchemaFields(listOf("id:INT64", "id:STRING")) }
    }
}
