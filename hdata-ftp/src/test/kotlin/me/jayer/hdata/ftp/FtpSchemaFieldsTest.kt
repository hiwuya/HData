package me.jayer.hdata.ftp

import org.apache.beam.sdk.schemas.Schema
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Pure-logic boundaries of `schema_fields` parsing.
 *
 * Before the refactor `parseSchemaField` was `else -> STRING`: typos like `age:intt` were silently
 * treated as STRING, only failing downstream. These branches must raise explicit errors, and must
 * actually catch the bad input.
 *
 * @author wuya
 */
class FtpSchemaFieldsTest {

    @Test
    fun `parses the field name and type`() {
        val (name, type) = parseSchemaField("age:int")
        assertEquals("age", name)
        assertEquals(Schema.TypeName.INT32, type.typeName)
    }

    @Test
    fun `recognizes all type aliases`() {
        assertEquals(Schema.FieldType.INT64, parseSchemaField("id:long").second)
        assertEquals(Schema.FieldType.INT64, parseSchemaField("id:int64").second)
        assertEquals(Schema.FieldType.FLOAT, parseSchemaField("f:float").second)
        assertEquals(Schema.FieldType.DOUBLE, parseSchemaField("d:double").second)
        assertEquals(Schema.FieldType.BOOLEAN, parseSchemaField("b:boolean").second)
        assertEquals(Schema.FieldType.BOOLEAN, parseSchemaField("b:bool").second)
        assertEquals(Schema.FieldType.STRING, parseSchemaField("s:string").second)
    }

    @Test
    fun `defaults to string when no type is given`() {
        assertEquals(Schema.FieldType.STRING, parseSchemaField("note").second)
    }

    @Test
    fun `field name must not be empty`() {
        val e = assertFailsWith<IllegalArgumentException> { parseSchemaField(":int") }
        assertTrue("must have a non-empty field name" in e.message!!)
    }

    @Test
    fun `an unknown type raises an error listing the valid values`() {
        val e = assertFailsWith<IllegalArgumentException> { parseSchemaField("x:intt") }
        assertTrue("intt" in e.message!!)
        assertTrue("string" in e.message!!)
    }

    @Test
    fun `csv schema is built from schema_fields`() {
        val schema = buildReadSchema(
            FtpReadConfig(host = "h", path = "/in", fileFormat = "csv", schemaFields = listOf("name:string", "age:int")),
        )
        assertEquals(Schema.TypeName.STRING, schema.getField("name").type.typeName)
        assertEquals(Schema.TypeName.INT32, schema.getField("age").type.typeName)
    }
}
