package me.jayer.hdata.filesystem

import org.apache.beam.sdk.schemas.Schema
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Pure-logic boundaries of `schema_fields` parsing.
 *
 * An unknown type **raises an error immediately** -- before the refactor it was `else -> STRING`,
 * so a typo like `age:intt` was silently treated as STRING and only blew up downstream when the
 * types did not line up.
 *
 * @author wuya
 */
class FilesystemSchemasTest {

    @Test
    fun `all supported types can be parsed`() {
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
    fun `an entry missing the colon raises an error`() {
        val e = assertFailsWith<IllegalArgumentException> { FilesystemSchemas.build(listOf("name")) }
        assertTrue("name:type" in e.message!!)
    }

    @Test
    fun `an empty field name raises an error`() {
        val e = assertFailsWith<IllegalArgumentException> { FilesystemSchemas.build(listOf(":string")) }
        assertTrue("non-empty field name" in e.message!!)
    }

    @Test
    fun `an unsupported type raises an error listing the valid values`() {
        val e = assertFailsWith<IllegalArgumentException> { FilesystemSchemas.build(listOf("x:intt")) }
        assertTrue("intt" in e.message!!)
        assertTrue("string" in e.message!!)
    }

    @Test
    fun `empty schema_fields uses the single-column text schema`() {
        assertEquals(FilesystemSchemas.TEXT_SCHEMA, FilesystemSchemas.build(emptyList()))
    }
}
