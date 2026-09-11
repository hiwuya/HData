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
 * Boundaries of [MongoRowCodec] still not covered by the existing tests:
 *  - the `document` field is null in document mode;
 *  - the error branches when a declared type does not match the actual value (getting these wrong only silently turns
 *    data into null/wrong, while the job still succeeds);
 *  - `MongoType.of` is case-insensitive.
 *
 * @author wuya
 */
class MongoRowCodecEdgeTest {

    private val codec = MongoRowCodec.of(
        listOf("id:STRING", "amount:DOUBLE", "qty:INT32", "blob:BYTES", "at:DATETIME", "ok:BOOLEAN")
    )

    @Test
    fun `in document mode a null document field fails fast`() {
        val plain = MongoRowCodec.of(emptyList())
        val row = Row.withSchema(
            Schema.builder().addNullableStringField(MongoRowCodec.DOCUMENT_FIELD).build()
        ).addValue(null).build()

        val error = assertFailsWith<IllegalArgumentException> { plain.toDocument(row) }

        assertTrue(MongoRowCodec.DOCUMENT_FIELD in error.message!!)
    }

    @Test
    fun `declared BOOLEAN but received a non-boolean value fails`() {
        val error = assertFailsWith<IllegalArgumentException> { codec.toRow(Document("ok", "yes")) }
        assertTrue("ok" in error.message!! && "BOOLEAN" in error.message!!)
    }

    @Test
    fun `declared DATETIME but received a non-temporal value fails`() {
        val error = assertFailsWith<IllegalArgumentException> { codec.toRow(Document("at", "2020")) }
        assertTrue("at" in error.message!! && "DATETIME" in error.message!!)
    }

    @Test
    fun `declared BYTES but received a non-byte value fails`() {
        val error = assertFailsWith<IllegalArgumentException> { codec.toRow(Document("blob", "x")) }
        assertTrue("blob" in error.message!! && "BYTES" in error.message!!)
    }

    @Test
    fun `on write, declared DATETIME but received a non-temporal value fails`() {
        // Beam Row itself rejects fields of the wrong type, so we hit the codec's toBson branch directly;
        // if this branch silently degrades, all timestamps written to ES/Mongo would become wrong
        val error = assertFailsWith<IllegalArgumentException> { MongoType.DATETIME.toBson("at", "not-a-date") }
        assertTrue("at" in error.message!! && "DATETIME" in error.message!!)
    }

    @Test
    fun `MongoType of is case-insensitive`() {
        assertEquals(MongoType.STRING, MongoType.of("string"))
        assertEquals(MongoType.INT32, MongoType.of("INT32"))
        assertEquals(MongoType.DATETIME, MongoType.of("datetime"))
    }

    @Test
    fun `DATETIME accepts both java util Date and java time Instant`() {
        val row1 = codec.toRow(Document("at", java.util.Date(1_700_000_000_000L)))
        assertEquals(1_700_000_000_000L, row1.getDateTime("at")!!.millis)

        val row2 = codec.toRow(Document("at", JavaInstant.ofEpochMilli(1_700_000_000_000L)))
        assertEquals(1_700_000_000_000L, row2.getDateTime("at")!!.millis)
    }
}
