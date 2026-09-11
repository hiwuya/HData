package me.jayer.hdata.core.util

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spec.SpecMappers
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes
import tools.jackson.databind.JsonNode
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.Base64
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * @author wuya
 * @date 2022-08-30
 */
class RowConvertersTest {

    private fun json(text: String): JsonNode = SpecMappers.CONFIG.readTree(text)

    private fun elements(vararg texts: String): List<JsonNode> = texts.map { json(it) }

    // ---------- inferSchema ----------

    @Test
    fun `integers are inferred as INT64 and floats as DOUBLE`() {
        val schema = RowConverters.inferSchema(elements("""{"i": 1, "d": 1.5, "b": true, "s": "x"}"""))

        assertEquals(Schema.FieldType.INT64, schema.getField("i").type)
        assertEquals(Schema.FieldType.DOUBLE, schema.getField("d").type)
        assertEquals(Schema.FieldType.BOOLEAN, schema.getField("b").type)
        assertEquals(Schema.FieldType.STRING, schema.getField("s").type)
        assertTrue(schema.fields.none { it.type.nullable })
    }

    @Test
    fun `mixing integers and floats unifies to DOUBLE`() {
        val schema = RowConverters.inferSchema(elements("""{"v": 1}""", """{"v": 1.5}"""))
        assertEquals(Schema.FieldType.DOUBLE, schema.getField("v").type)
    }

    @Test
    fun `a missing field or an explicit null makes that field nullable`() {
        val schema = RowConverters.inferSchema(elements("""{"a": 1, "b": 2}""", """{"a": 3, "b": null}""", """{"a": 4}"""))

        assertTrue(!schema.getField("a").type.nullable)
        assertTrue(schema.getField("b").type.nullable)
        // field order follows first appearance
        assertEquals(listOf("a", "b"), schema.fieldNames)
    }

    @Test
    fun `an all-null field falls back to a nullable string`() {
        val schema = RowConverters.inferSchema(elements("""{"a": null}"""))
        assertEquals(Schema.FieldType.STRING.withNullable(true), schema.getField("a").type)
    }

    @Test
    fun `nested objects and arrays can be inferred`() {
        val schema = RowConverters.inferSchema(elements("""{"user": {"id": 1}, "tags": ["a", "b"]}"""))

        assertEquals(Schema.TypeName.ROW, schema.getField("user").type.typeName)
        assertEquals(Schema.FieldType.INT64, schema.getField("user").type.rowSchema!!.getField("id").type)
        assertEquals(Schema.FieldType.array(Schema.FieldType.STRING), schema.getField("tags").type)
    }

    @Test
    fun `an array containing null has a nullable element type and converts directly`() {
        val elements = elements("""{"values": [1, null, 3]}""")
        val schema = RowConverters.inferSchema(elements)

        assertEquals(
            Schema.FieldType.array(Schema.FieldType.INT64.withNullable(true)),
            schema.getField("values").type,
        )
        val row = RowConverters.toRow(schema, elements.single())
        assertEquals(listOf(1L, null, 3L), row.getArray<Long?>("values"))
    }

    @Test
    fun `an all-null array falls back to nullable string elements`() {
        val elements = elements("""{"values": [null, null]}""")
        val schema = RowConverters.inferSchema(elements)

        assertEquals(
            Schema.FieldType.array(Schema.FieldType.STRING.withNullable(true)),
            schema.getField("values").type,
        )
        assertEquals(listOf(null, null), RowConverters.toRow(schema, elements.single()).getArray<String?>("values"))
    }

    @Test
    fun `inconsistent types for the same field error out`() {
        val error = assertFailsWith<HDataException> {
            RowConverters.inferSchema(elements("""{"v": 1}""", """{"v": "x"}"""))
        }
        assertTrue("inconsistent value types" in error.message!!)
    }

    @Test
    fun `a non-object record errors out and points out the location`() {
        val error = assertFailsWith<HDataException> {
            RowConverters.inferSchema(elements("""{"a": 1}""", """[1, 2]"""), "elements")
        }
        assertTrue("elements[1]" in error.message!!)
    }

    // ---------- toRow ----------

    @Test
    fun `converts scalar and nullable fields according to the schema`() {
        val schema = Schema.builder()
            .addInt32Field("i")
            .addNullableStringField("s")
            .addBooleanField("b")
            .build()

        val row = RowConverters.toRow(schema, json("""{"i": 7, "s": null, "b": true}"""))

        assertEquals(7, row.getInt32("i"))
        assertNull(row.getString("s"))
        assertEquals(true, row.getBoolean("b"))
    }

    @Test
    fun `a missing nullable field is treated as null`() {
        val schema = Schema.builder().addStringField("a").addNullableInt64Field("b").build()
        val row = RowConverters.toRow(schema, json("""{"a": "x"}"""))
        assertNull(row.getInt64("b"))
    }

    @Test
    fun `a missing value for a non-nullable field errors out and points out the path`() {
        val schema = Schema.builder().addStringField("a").addInt64Field("b").build()
        val error = assertFailsWith<HDataException> { RowConverters.toRow(schema, json("""{"a": "x"}""")) }
        assertTrue("$.b" in error.message!! && "must not be null" in error.message!!)
    }

    @Test
    fun `unknown fields are rejected rather than ignored`() {
        val schema = Schema.builder().addStringField("a").build()
        val error = assertFailsWith<HDataException> { RowConverters.toRow(schema, json("""{"a": "x", "typo": 1}""")) }
        assertTrue("typo" in error.message!!)
    }

    @Test
    fun `a type mismatch errors out`() {
        val schema = Schema.builder().addInt64Field("a").build()
        val error = assertFailsWith<HDataException> { RowConverters.toRow(schema, json("""{"a": "x"}""")) }
        assertTrue("expects a number" in error.message!!)
    }

    @Test
    fun `strings and booleans are not silently coerced`() {
        val stringSchema = Schema.builder().addStringField("v").build()
        val booleanSchema = Schema.builder().addBooleanField("v").build()

        assertFailsWith<HDataException> { RowConverters.toRow(stringSchema, json("""{"v": 1}""")) }
        val error = assertFailsWith<HDataException> {
            RowConverters.toRow(booleanSchema, json("""{"v": "true"}"""))
        }
        assertTrue("$.v" in error.message!! && "boolean" in error.message!!)
    }

    @Test
    fun `an invalid base64 error keeps the field path`() {
        val schema = Schema.builder().addByteArrayField("payload").build()
        val error = assertFailsWith<HDataException> {
            RowConverters.toRow(schema, json("""{"payload": "%%%"}"""))
        }
        assertTrue("$.payload" in error.message!! && "base64" in error.message!!)
    }

    @Test
    fun `an out-of-range or fractional integer is rejected rather than truncated`() {
        val byteSchema = Schema.builder().addByteField("v").build()
        assertFailsWith<HDataException> { RowConverters.toRow(byteSchema, json("""{"v": 128}""")) }
        assertFailsWith<HDataException> { RowConverters.toRow(byteSchema, json("""{"v": 1.5}""")) }

        val intSchema = Schema.builder().addInt32Field("v").build()
        val error = assertFailsWith<HDataException> {
            RowConverters.toRow(intSchema, json("""{"v": 2147483648}"""))
        }
        assertTrue("losslessly converted" in error.message!!)
    }

    @Test
    fun `a float overflow rejects infinity`() {
        val schema = Schema.builder().addFloatField("v").build()
        val error = assertFailsWith<HDataException> {
            RowConverters.toRow(schema, json("""{"v": 1e1000}"""))
        }
        assertTrue("finite range" in error.message!!)
    }

    @Test
    fun `DECIMAL and BYTES are parsed as a string and base64 respectively`() {
        val schema = Schema.builder()
            .addDecimalField("amount")
            .addByteArrayField("payload")
            .build()
        val payload = Base64.getEncoder().encodeToString("hi".toByteArray())

        val row = RowConverters.toRow(schema, json("""{"amount": "1.05", "payload": "$payload"}"""))

        assertEquals(BigDecimal("1.05"), row.getDecimal("amount"))
        assertContentEquals("hi".toByteArray(), row.getBytes("payload"))
    }

    @Test
    fun `logical time types are parsed from ISO text`() {
        val schema = Schema.builder()
            .addLogicalTypeField("d", SqlTypes.DATE)
            .addLogicalTypeField("t", SqlTypes.TIME)
            .addLogicalTypeField("dt", SqlTypes.DATETIME)
            .addLogicalTypeField("ts", SqlTypes.TIMESTAMP)
            .build()

        val row = RowConverters.toRow(
            schema,
            json(
                """
                {"d": "2022-08-30", "t": "12:30:00", "dt": "2022-08-30T12:30:00", "ts": "2022-08-30T12:30:00Z"}
                """.trimIndent()
            ),
        )

        assertEquals(LocalDate.of(2022, 8, 30), row.getLogicalTypeValue("d", LocalDate::class.java))
        assertEquals(LocalTime.of(12, 30), row.getLogicalTypeValue("t", LocalTime::class.java))
        assertEquals(LocalDateTime.of(2022, 8, 30, 12, 30), row.getLogicalTypeValue("dt", LocalDateTime::class.java))
        assertEquals(Instant.parse("2022-08-30T12:30:00Z"), row.getLogicalTypeValue("ts", Instant::class.java))
    }

    @Test
    fun `invalid time text errors out and includes the original value`() {
        val schema = Schema.builder().addLogicalTypeField("d", SqlTypes.DATE).build()
        val error = assertFailsWith<HDataException> { RowConverters.toRow(schema, json("""{"d": "not-a-date"}""")) }
        assertTrue("not-a-date" in error.message!!)
    }

    @Test
    fun `logical time types do not accept implicit string conversion of numeric nodes`() {
        val schema = Schema.builder().addLogicalTypeField("d", SqlTypes.DATE).build()
        val error = assertFailsWith<HDataException> { RowConverters.toRow(schema, json("""{"d": 20220830}""")) }
        assertTrue("$.d" in error.message!! && "datetime string" in error.message!!)
    }

    @Test
    fun `nested ROWs and arrays are converted recursively by element type`() {
        val inner = Schema.builder().addInt64Field("id").build()
        val schema = Schema.builder()
            .addRowField("user", inner)
            .addArrayField("scores", Schema.FieldType.INT64)
            .build()

        val row = RowConverters.toRow(schema, json("""{"user": {"id": 9}, "scores": [1, 2, 3]}"""))

        assertEquals(9L, row.getRow("user")!!.getInt64("id"))
        assertEquals(listOf(1L, 2L, 3L), row.getArray<Long>("scores"))
    }

    @Test
    fun `an array element error includes the index in the path`() {
        val schema = Schema.builder().addArrayField("scores", Schema.FieldType.INT64).build()
        val error = assertFailsWith<HDataException> { RowConverters.toRow(schema, json("""{"scores": [1, "x"]}""")) }
        assertTrue("$.scores[1]" in error.message!!)
    }

    @Test
    fun `an inferred schema can be used directly for conversion`() {
        val elements = elements("""{"id": 1, "name": "a"}""", """{"id": 2, "name": null}""")
        val schema = RowConverters.inferSchema(elements)

        val rows = elements.map { RowConverters.toRow(schema, it) }

        assertEquals(listOf(1L, 2L), rows.map { it.getInt64("id") })
        assertEquals(listOf("a", null), rows.map { it.getString("name") })
    }

    // ---------- boundaries of parsing / inference ----------

    @Test
    fun `toRow errors out when it expects an object but is given a non-object`() {
        val schema = Schema.builder().addStringField("a").build()
        val error = assertFailsWith<HDataException> { RowConverters.toRow(schema, json("""[1, 2]""")) }
        assertTrue("expects an object" in error.message!!)
    }

    @Test
    fun `inferSchema rejects an empty list`() {
        val error = assertFailsWith<IllegalArgumentException> { RowConverters.inferSchema(emptyList()) }
        assertTrue("empty list" in error.message!!)
    }

    @Test
    fun `inferSchema rejects a record with no fields`() {
        val error = assertFailsWith<HDataException> { RowConverters.inferSchema(elements("""{}""")) }
        assertTrue("has no fields" in error.message!!)
    }

    @Test
    fun `an array field errors out when it expects an array but is given a non-array`() {
        val schema = Schema.builder().addArrayField("scores", Schema.FieldType.INT64).build()
        val error = assertFailsWith<HDataException> { RowConverters.toRow(schema, json("""{"scores": "x"}""")) }
        assertTrue("expects an array" in error.message!!)
    }

    @Test
    fun `map 字段期望对象但给了非对象时报错`() {
        val schema = Schema.builder()
            .addMapField("m", Schema.FieldType.STRING, Schema.FieldType.STRING)
            .build()
        val error = assertFailsWith<HDataException> { RowConverters.toRow(schema, json("""{"m": "x"}""")) }
        assertTrue("expects an object" in error.message!!)
    }
}
