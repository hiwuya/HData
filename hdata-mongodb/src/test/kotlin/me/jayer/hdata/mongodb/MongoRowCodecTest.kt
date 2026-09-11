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
 * Conversion between Bson Document and Beam Row.
 *
 * @author wuya
 */
class MongoRowCodecTest {

    private val codec = MongoRowCodec.of(
        listOf("id:STRING", "amount:DOUBLE", "qty:INT32", "big:INT64", "ok:BOOLEAN", "at:DATETIME", "blob:BYTES")
    )

    @Test
    fun `document is read into a row, missing fields become null`() {
        val row = codec.toRow(Document("id", "a1").append("amount", 12.5))

        assertEquals("a1", row.getString("id"))
        assertEquals(12.5, row.getDouble("amount"))
        assertNull(row.getInt32("qty"))
    }

    @Test
    fun `numeric types convert leniently among themselves`() {
        // MongoDB has no schema, so whether a field is stored as Int or Long across documents depends entirely on the writer;
        // before the refactor we used doc.getInteger(name), which threw ClassCastException on Long and killed the whole job
        val row = codec.toRow(Document("qty", 7L).append("big", 3).append("amount", 5))

        assertEquals(7, row.getInt32("qty"))
        assertEquals(3L, row.getInt64("big"))
        assertEquals(5.0, row.getDouble("amount"))
    }

    @Test
    fun `a completely mismatched type fails and names the offending field`() {
        val error = assertFailsWith<IllegalArgumentException> {
            codec.toRow(Document("qty", "seven"))
        }

        assertTrue("qty" in error.message!! && "INT32" in error.message!!)
    }

    @Test
    fun `fractional and out-of-range integers are rejected rather than truncated or wrapped`() {
        assertFailsWith<IllegalArgumentException> { codec.toRow(Document("qty", 1.5)) }
        assertFailsWith<IllegalArgumentException> { codec.toRow(Document("qty", 2_147_483_648L)) }
    }

    @Test
    fun `row and document round-trip in both directions preserving values`() {
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
    fun `without schema_fields read and write use the same column name`() {
        // Before the refactor the reader produced a document column while the writer looked for a value column,
        // so plugging ReadFromMongoDb's output straight into WriteToMongoDb reported "missing field"
        val plain = MongoRowCodec.of(emptyList())

        val row = plain.toRow(Document("a", 1).append("b", "x"))
        val back = plain.toDocument(row)

        assertEquals(MongoRowCodec.DOCUMENT_FIELD, plain.schema.getField(0).name)
        assertEquals(1, back["a"])
        assertEquals("x", back["b"])
    }

    @Test
    fun `in document mode a missing column reports the correct field name`() {
        val plain = MongoRowCodec.of(emptyList())
        val row = Row.withSchema(Schema.builder().addNullableStringField("value").build()).addValue("{}").build()

        val error = assertFailsWith<IllegalArgumentException> { plain.toDocument(row) }

        assertTrue(MongoRowCodec.DOCUMENT_FIELD in error.message!!)
    }

    @Test
    fun `a written row missing a declared field fails`() {
        val row = Row.withSchema(Schema.builder().addNullableStringField("id").build()).addValue("a1").build()

        val error = assertFailsWith<IllegalArgumentException> { codec.toDocument(row) }

        assertTrue("amount" in error.message!!)
    }

    @Test
    fun `projection only requests the declared fields`() {
        val projection = MongoRowCodec.of(listOf("id:STRING", "amount:DOUBLE")).projection()

        assertEquals(setOf("id", "amount"), projection!!.keys)
        // In document mode we need the whole document, so no projection can be applied
        assertNull(MongoRowCodec.of(emptyList()).projection())
    }

    @Test
    fun `the codec can be serialized and shipped along with the DoFn`() {
        SerializableUtils.ensureSerializable(codec)
    }

    @Test
    fun `a malformed schema_fields entry fails`() {
        assertFailsWith<IllegalArgumentException> { parseSchemaFields(listOf("id")) }
        assertFailsWith<IllegalArgumentException> { parseSchemaFields(listOf(":STRING")) }
        assertFailsWith<IllegalArgumentException> { parseSchemaFields(listOf("id:UUID")) }
        assertFailsWith<IllegalArgumentException> { parseSchemaFields(listOf("id:INT64", "id:STRING")) }
    }
}
