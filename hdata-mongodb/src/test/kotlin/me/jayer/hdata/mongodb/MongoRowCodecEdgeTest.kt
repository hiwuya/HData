package me.jayer.hdata.mongodb

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import org.bson.Document
import org.joda.time.Instant as JodaInstant
import java.time.Instant as JavaInstant
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * [MongoRowCodec] 在现有测试之外仍然没被触达的边界：
 *  - document 模式里 `document` 字段为 null；
 *  - 各类型声明与实际值对不上时的报错分支（这些分支算错只会让数据静默变成 null/错的，作业照样成功）；
 *  - `MongoType.of` 大小写不敏感。
 *
 * @author wuya
 */
class MongoRowCodecEdgeTest {

    private val codec = MongoRowCodec.of(
        listOf("id:STRING", "amount:DOUBLE", "qty:INT32", "blob:BYTES", "at:DATETIME", "ok:BOOLEAN")
    )

    @Test
    fun `document 模式下 document 字段为 null 时直接报错`() {
        val plain = MongoRowCodec.of(emptyList())
        val row = Row.withSchema(
            Schema.builder().addNullableStringField(MongoRowCodec.DOCUMENT_FIELD).build()
        ).addValue(null).build()

        val error = assertFailsWith<IllegalArgumentException> { plain.toDocument(row) }

        assertTrue(MongoRowCodec.DOCUMENT_FIELD in error.message!!)
    }

    @Test
    fun `BOOLEAN 声明但拿到非布尔值时报错`() {
        val error = assertFailsWith<IllegalArgumentException> { codec.toRow(Document("ok", "yes")) }
        assertTrue("ok" in error.message!! && "BOOLEAN" in error.message!!)
    }

    @Test
    fun `DATETIME 声明但拿到非时间值时报错`() {
        val error = assertFailsWith<IllegalArgumentException> { codec.toRow(Document("at", "2020")) }
        assertTrue("at" in error.message!! && "DATETIME" in error.message!!)
    }

    @Test
    fun `BYTES 声明但拿到非字节值时报错`() {
        val error = assertFailsWith<IllegalArgumentException> { codec.toRow(Document("blob", "x")) }
        assertTrue("blob" in error.message!! && "BYTES" in error.message!!)
    }

    @Test
    fun `写入时 DATETIME 声明但拿到非时间值时报错`() {
        // Beam Row 自身会拦掉类型不符的字段，所以直接打 codec 的 toBson 分支，
        // 这条分支一旦悄悄退化，写进 ES/Mongo 的时间就全成了错的
        val error = assertFailsWith<IllegalArgumentException> { MongoType.DATETIME.toBson("at", "not-a-date") }
        assertTrue("at" in error.message!! && "DATETIME" in error.message!!)
    }

    @Test
    fun `MongoType_of 大小写不敏感`() {
        assertEquals(MongoType.STRING, MongoType.of("string"))
        assertEquals(MongoType.INT32, MongoType.of("INT32"))
        assertEquals(MongoType.DATETIME, MongoType.of("datetime"))
    }

    @Test
    fun `DATETIME 支持 java util Date 与 java time Instant`() {
        val row1 = codec.toRow(Document("at", java.util.Date(1_700_000_000_000L)))
        assertEquals(1_700_000_000_000L, row1.getDateTime("at")!!.millis)

        val row2 = codec.toRow(Document("at", JavaInstant.ofEpochMilli(1_700_000_000_000L)))
        assertEquals(1_700_000_000_000L, row2.getDateTime("at")!!.millis)
    }
}
