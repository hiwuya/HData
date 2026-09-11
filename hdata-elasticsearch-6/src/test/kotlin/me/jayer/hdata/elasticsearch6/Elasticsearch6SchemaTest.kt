package me.jayer.hdata.elasticsearch6

import org.apache.beam.sdk.schemas.Schema
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.util.Base64

/**
 * The parts of the ES 6.x pure-logic layer that previously had zero coverage: `schema_fields` parsing, Beam schema
 * construction, and the write side's `esValue` field type conversion (DATETIME→epochMillis, BYTES→base64, etc.).
 *
 * `esValue` is precisely the link where "the config option really takes effect": once `schema_fields` is configured, row
 * values must be converted per type into JSON-friendly values ES can serialize; a wrong type or a silently dropped
 * conversion only makes the data not line up.
 *
 * @author wuya
 */
class Elasticsearch6SchemaTest {

    @Test
    fun `parseSchemaFields is case-insensitive for types`() {
        assertEquals(EsField("id", EsFieldType.INT64), parseSchemaFields(listOf("id:int64")).single())
    }

    @Test
    fun `parseSchemaFields errors on an unsupported type`() {
        assertThrows(IllegalArgumentException::class.java) { parseSchemaFields(listOf("id:UUID")) }
    }

    @Test
    fun `parseSchemaFields errors when the colon is missing`() {
        assertThrows(IllegalArgumentException::class.java) { parseSchemaFields(listOf("id")) }
    }

    @Test
    fun `with no schema_fields given, the read side defaults to a single document column`() {
        assertEquals("document", DOCUMENT_SCHEMA.getField(0).name)
        assertEquals(Schema.TypeName.STRING, DOCUMENT_SCHEMA.getField("document").type.typeName)
    }

    @Test
    fun `buildSchema builds fields per their type`() {
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
    fun `esValue converts each type into a JSON-friendly value`() {
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
    fun `an invalid value is rejected instead of being truncated or silently turned into false or null`() {
        assertThrows(IllegalArgumentException::class.java) { esValue(EsFieldType.INT32, 1.5) }
        assertThrows(IllegalArgumentException::class.java) { esValue(EsFieldType.INT32, 2_147_483_648L) }
        assertThrows(IllegalArgumentException::class.java) { esValue(EsFieldType.BOOLEAN, "maybe") }
        assertThrows(IllegalArgumentException::class.java) { esValue(EsFieldType.BYTES, "not-base64!") }
        assertThrows(IllegalArgumentException::class.java) { esRowValue(EsFieldType.BYTES, "not-base64!") }
    }
}
