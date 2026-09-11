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
 * The part of ES 8.x's pure logic layer that previously had zero coverage: the read-side `convertValue`
 * and write-side `toJsonValue` type conversions, `schema_fields` parsing and Beam schema construction,
 * and type validation.
 *
 * These conversions are the part that makes "the config actually takes effect" true: once `schema_fields`
 * is configured, the values in `_source` must be converted to whatever Beam Row / JSON accepts per the
 * declared type — converting to the wrong type or silently dropping the conversion just makes the data
 * not line up.
 *
 * @author wuya
 */
class EsSchemaTest {

    @Test
    fun `parseSchemaFields is case-insensitive`() {
        assertEquals(listOf("id" to "STRING"), parseSchemaFields(listOf("id:string")))
    }

    @Test
    fun `parseSchemaFields errors when the colon is missing`() {
        assertThrows(IllegalArgumentException::class.java) { parseSchemaFields(listOf("id")) }
        assertThrows(IllegalArgumentException::class.java) { parseSchemaFields(listOf(":INT64")) }
    }

    @Test
    fun `fieldType errors on an unsupported type`() {
        assertThrows(IllegalArgumentException::class.java) { fieldType("UUID") }
    }

    @Test
    fun `buildSchema degrades to a document column when fields are empty`() {
        val schema = buildSchema(emptyList())
        assertEquals("document", schema.getField(0).name)
    }

    @Test
    fun `buildSchema builds fields by type`() {
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
    fun `convertValue converts by type on the read side`() {
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
    fun `toJsonValue converts by type on the write side`() {
        assertNull(toJsonValue(null, "DATETIME"))
        assertEquals("hi", toJsonValue("hi", "STRING"))
        val at = JodaInstant.ofEpochMilli(1_700_000_000_000L)
        assertEquals(at.toString(), toJsonValue(at, "DATETIME"))
        assertEquals(Base64.getEncoder().encodeToString(byteArrayOf(1, 2, 3)), toJsonValue(byteArrayOf(1, 2, 3), "BYTES"))
    }

    @Test
    fun `an invalid value is rejected instead of becoming null, false, or getting truncated`() {
        assertThrows(IllegalArgumentException::class.java) { convertValue("maybe", "BOOLEAN") }
        assertThrows(IllegalArgumentException::class.java) { convertValue("not-base64!", "BYTES") }
        assertThrows(IllegalArgumentException::class.java) { convertValue(1.5, "INT32") }
        assertThrows(IllegalArgumentException::class.java) { convertValue(2_147_483_648L, "INT32") }
        assertThrows(IllegalArgumentException::class.java) { toJsonValue("not-bytes", "BYTES") }
        assertThrows(IllegalArgumentException::class.java) { toJsonValue("2026-01-01", "DATETIME") }
    }
}
