package me.jayer.hdata.dynamodb

import me.jayer.hdata.dynamodb.internal.DynamoDBTypeMappings
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import org.junit.jupiter.api.Test
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import java.math.BigDecimal
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Comprehensive tests for [DynamoDBTypeMappings]: schema derivation, DynamoDB-to-Beam
 * conversion, Beam-to-DynamoDB conversion, round-trip fidelity, and the convenience
 * [DynamoDBTypeMappings.toAttributeValue] overload.
 */
class DynamoDBTypeMappingsTest {

    // =====================================================================
    // 1. deriveSchema
    // =====================================================================

    @Test
    fun `deriveSchema - empty items returns empty schema`() {
        val schema = DynamoDBTypeMappings.deriveSchema(emptyList())
        assertEquals(0, schema.fieldCount)
    }

    @Test
    fun `deriveSchema - single item with S N BOOL attributes`() {
        val item = mapOf(
            "name" to AttributeValue.builder().s("Alice").build(),
            "age" to AttributeValue.builder().n("30").build(),
            "active" to AttributeValue.builder().bool(true).build(),
        )
        val schema = DynamoDBTypeMappings.deriveSchema(listOf(item))
        assertEquals(3, schema.fieldCount)
        // Fields are sorted alphabetically
        assertEquals("active", schema.getField(0).name)
        assertEquals(Schema.FieldType.BOOLEAN.withNullable(true), schema.getField(0).type)
        assertEquals("age", schema.getField(1).name)
        assertEquals(Schema.FieldType.DECIMAL.withNullable(true), schema.getField(1).type)
        assertEquals("name", schema.getField(2).name)
        assertEquals(Schema.FieldType.STRING.withNullable(true), schema.getField(2).type)
    }

    @Test
    fun `deriveSchema - mixed types across items are promoted`() {
        val item1 = mapOf(
            "data" to AttributeValue.builder().s("hello").build(),
        )
        val item2 = mapOf(
            "data" to AttributeValue.builder().n("42").build(),
        )
        val schema = DynamoDBTypeMappings.deriveSchema(listOf(item1, item2))
        assertEquals(1, schema.fieldCount)
        // STRING (from S) + DECIMAL (from N) -> DECIMAL (STRING treated as NUL placeholder)
        assertEquals(Schema.FieldType.DECIMAL.withNullable(true), schema.getField(0).type)
    }

    @Test
    fun `deriveSchema - NUL placeholder is promoted when a concrete type exists`() {
        val item1 = mapOf(
            "score" to AttributeValue.builder().nul(true).build(),
        )
        val item2 = mapOf(
            "score" to AttributeValue.builder().n("99.5").build(),
        )
        val schema = DynamoDBTypeMappings.deriveSchema(listOf(item1, item2))
        assertEquals(1, schema.fieldCount)
        assertEquals(Schema.FieldType.DECIMAL.withNullable(true), schema.getField(0).type)
    }

    @Test
    fun `deriveSchema - nullable fields when attribute absent in some items`() {
        val item1 = mapOf(
            "id" to AttributeValue.builder().s("1").build(),
            "extra" to AttributeValue.builder().s("x").build(),
        )
        val item2 = mapOf(
            "id" to AttributeValue.builder().s("2").build(),
            // "extra" is absent here
        )
        val schema = DynamoDBTypeMappings.deriveSchema(listOf(item1, item2))
        assertEquals(2, schema.fieldCount)
        // All fields are nullable because DynamoDB is schemaless
        assertTrue(schema.getField(0).type.nullable)
        assertTrue(schema.getField(1).type.nullable)
    }

    @Test
    fun `deriveSchema - binary type`() {
        val item = mapOf(
            "blob" to AttributeValue.builder().b(SdkBytes.fromByteArray(byteArrayOf(1, 2, 3))).build(),
        )
        val schema = DynamoDBTypeMappings.deriveSchema(listOf(item))
        assertEquals(1, schema.fieldCount)
        assertEquals(Schema.FieldType.BYTES.withNullable(true), schema.getField(0).type)
    }

    @Test
    fun `deriveSchema - list and map types`() {
        val item = mapOf(
            "tags" to AttributeValue.builder().l(
                listOf(
                    AttributeValue.builder().s("a").build(),
                    AttributeValue.builder().s("b").build(),
                ),
            ).build(),
            "meta" to AttributeValue.builder().m(
                mapOf("k1" to AttributeValue.builder().s("v1").build()),
            ).build(),
        )
        val schema = DynamoDBTypeMappings.deriveSchema(listOf(item))
        assertEquals(2, schema.fieldCount)
        // Fields are sorted alphabetically: "meta" before "tags"
        assertEquals(Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.STRING).withNullable(true), schema.getField(0).type)
        assertEquals(Schema.FieldType.array(Schema.FieldType.STRING).withNullable(true), schema.getField(1).type)
    }

    @Test
    fun `deriveSchema - string set and number set`() {
        val item = mapOf(
            "names" to AttributeValue.builder().ss(listOf("a", "b")).build(),
            "nums" to AttributeValue.builder().ns(listOf("1", "2")).build(),
        )
        val schema = DynamoDBTypeMappings.deriveSchema(listOf(item))
        assertEquals(2, schema.fieldCount)
        assertEquals(Schema.FieldType.array(Schema.FieldType.STRING).withNullable(true), schema.getField(0).type)
        assertEquals(Schema.FieldType.array(Schema.FieldType.STRING).withNullable(true), schema.getField(1).type)
    }

    // =====================================================================
    // 2. itemToRow
    // =====================================================================

    private fun buildSchema(vararg fields: Pair<String, Schema.FieldType>): Schema {
        val builder = Schema.builder()
        for ((name, type) in fields) {
            builder.addNullableField(name, type)
        }
        return builder.build()
    }

    @Test
    fun `itemToRow - S to String`() {
        val schema = buildSchema("name" to Schema.FieldType.STRING)
        val item = mapOf("name" to AttributeValue.builder().s("Alice").build())
        val row = DynamoDBTypeMappings.itemToRow(item, schema)
        assertEquals("Alice", row.getString("name"))
    }

    @Test
    fun `itemToRow - N to String when schema is STRING`() {
        // When schema field is STRING but DynamoDB value is N, toBeamValue returns the number as string
        val schema = buildSchema("age" to Schema.FieldType.STRING)
        val item = mapOf("age" to AttributeValue.builder().n("42").build())
        val row = DynamoDBTypeMappings.itemToRow(item, schema)
        assertEquals("42", row.getString("age"))
    }

    @Test
    fun `itemToRow - N to BigDecimal`() {
        val schema = buildSchema("amount" to Schema.FieldType.DECIMAL)
        val item = mapOf("amount" to AttributeValue.builder().n("123.456").build())
        val row = DynamoDBTypeMappings.itemToRow(item, schema)
        assertEquals(BigDecimal("123.456"), row.getValue<BigDecimal>("amount"))
    }

    @Test
    fun `itemToRow - B to ByteArray`() {
        val schema = buildSchema("blob" to Schema.FieldType.BYTES)
        val bytes = byteArrayOf(0x01, 0x02, 0x03)
        val item = mapOf("blob" to AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build())
        val row = DynamoDBTypeMappings.itemToRow(item, schema)
        assertContentEquals(bytes, row.getBytes("blob"))
    }

    @Test
    fun `itemToRow - BOOL to Boolean`() {
        val schema = buildSchema("flag" to Schema.FieldType.BOOLEAN)
        val item = mapOf("flag" to AttributeValue.builder().bool(true).build())
        val row = DynamoDBTypeMappings.itemToRow(item, schema)
        assertEquals(true, row.getBoolean("flag"))
    }

    @Test
    fun `itemToRow - NUL to null`() {
        val schema = buildSchema("opt" to Schema.FieldType.STRING)
        val item = mapOf("opt" to AttributeValue.builder().nul(true).build())
        val row = DynamoDBTypeMappings.itemToRow(item, schema)
        assertNull(row.getString("opt"))
    }

    @Test
    fun `itemToRow - missing attribute to null`() {
        val schema = buildSchema("missing" to Schema.FieldType.STRING)
        val item = emptyMap<String, AttributeValue>()
        val row = DynamoDBTypeMappings.itemToRow(item, schema)
        assertNull(row.getString("missing"))
    }

    @Test
    fun `itemToRow - L list to List of Strings`() {
        val schema = buildSchema("tags" to Schema.FieldType.array(Schema.FieldType.STRING))
        val item = mapOf("tags" to AttributeValue.builder().l(
            listOf(
                AttributeValue.builder().s("x").build(),
                AttributeValue.builder().s("y").build(),
            ),
        ).build())
        val row = DynamoDBTypeMappings.itemToRow(item, schema)
        assertEquals(listOf("x", "y"), row.getValue<List<String>>("tags"))
    }

    @Test
    fun `itemToRow - SS string set to List of Strings`() {
        val schema = buildSchema("names" to Schema.FieldType.array(Schema.FieldType.STRING))
        val item = mapOf("names" to AttributeValue.builder().ss(listOf("a", "b", "c")).build())
        val row = DynamoDBTypeMappings.itemToRow(item, schema)
        assertEquals(listOf("a", "b", "c"), row.getValue<List<String>>("names"))
    }

    @Test
    fun `itemToRow - NS number set to List of Strings`() {
        val schema = buildSchema("nums" to Schema.FieldType.array(Schema.FieldType.STRING))
        val item = mapOf("nums" to AttributeValue.builder().ns(listOf("10", "20")).build())
        val row = DynamoDBTypeMappings.itemToRow(item, schema)
        assertEquals(listOf("10", "20"), row.getValue<List<String>>("nums"))
    }

    @Test
    fun `itemToRow - M map to Map of Strings`() {
        val schema = buildSchema("meta" to Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.STRING))
        val item = mapOf("meta" to AttributeValue.builder().m(
            mapOf(
                "k1" to AttributeValue.builder().s("v1").build(),
                "k2" to AttributeValue.builder().s("v2").build(),
            ),
        ).build())
        val row = DynamoDBTypeMappings.itemToRow(item, schema)
        assertEquals(mapOf("k1" to "v1", "k2" to "v2"), row.getValue<Map<String, String>>("meta"))
    }

    @Test
    fun `itemToRow - BS binary set to list of byte arrays`() {
        val schema = buildSchema("blobs" to Schema.FieldType.array(Schema.FieldType.BYTES))
        val b1 = byteArrayOf(1, 2)
        val b2 = byteArrayOf(3, 4)
        val item = mapOf("blobs" to AttributeValue.builder().bs(
            SdkBytes.fromByteArray(b1),
            SdkBytes.fromByteArray(b2),
        ).build())
        val row = DynamoDBTypeMappings.itemToRow(item, schema)
        val list = row.getValue<List<ByteArray>>("blobs")
        assertEquals(2, list.size)
        assertContentEquals(b1, list[0])
        assertContentEquals(b2, list[1])
    }

    // =====================================================================
    // 3. rowToItem
    // =====================================================================

    @Test
    fun `rowToItem - null to nul`() {
        val schema = buildSchema("col" to Schema.FieldType.STRING)
        val row = Row.withSchema(schema).addValue(null).build()
        val item = DynamoDBTypeMappings.rowToItem(row)
        assertTrue(item["col"]!!.nul() == true)
    }

    @Test
    fun `rowToItem - String to S`() {
        val schema = buildSchema("name" to Schema.FieldType.STRING)
        val row = Row.withSchema(schema).addValue("hello").build()
        val item = DynamoDBTypeMappings.rowToItem(row)
        assertEquals(AttributeValue.builder().s("hello").build(), item["name"])
    }

    @Test
    fun `rowToItem - Int to N`() {
        val schema = buildSchema("count" to Schema.FieldType.INT32)
        val row = Row.withSchema(schema).addValue(42).build()
        val item = DynamoDBTypeMappings.rowToItem(row)
        assertEquals("42", item["count"]!!.n())
    }

    @Test
    fun `rowToItem - Long to N`() {
        val schema = buildSchema("big" to Schema.FieldType.INT64)
        val row = Row.withSchema(schema).addValue(9999999999L).build()
        val item = DynamoDBTypeMappings.rowToItem(row)
        assertEquals("9999999999", item["big"]!!.n())
    }

    @Test
    fun `rowToItem - Float to N`() {
        val schema = buildSchema("ratio" to Schema.FieldType.FLOAT)
        val row = Row.withSchema(schema).addValue(1.5f).build()
        val item = DynamoDBTypeMappings.rowToItem(row)
        assertNotNull(item["ratio"]!!.n())
    }

    @Test
    fun `rowToItem - Double to N`() {
        val schema = buildSchema("precise" to Schema.FieldType.DOUBLE)
        val row = Row.withSchema(schema).addValue(3.14159).build()
        val item = DynamoDBTypeMappings.rowToItem(row)
        assertEquals("3.14159", item["precise"]!!.n())
    }

    @Test
    fun `rowToItem - Boolean to BOOL`() {
        val schema = buildSchema("flag" to Schema.FieldType.BOOLEAN)
        val row = Row.withSchema(schema).addValue(true).build()
        val item = DynamoDBTypeMappings.rowToItem(row)
        assertEquals(AttributeValue.builder().bool(true).build(), item["flag"])
    }

    @Test
    fun `rowToItem - ByteArray to B`() {
        val schema = buildSchema("data" to Schema.FieldType.BYTES)
        val bytes = byteArrayOf(0x0A, 0x0B, 0x0C)
        val row = Row.withSchema(schema).addValue(bytes).build()
        val item = DynamoDBTypeMappings.rowToItem(row)
        assertContentEquals(bytes, item["data"]!!.b().asByteArray())
    }

    @Test
    fun `rowToItem - List to L`() {
        val schema = buildSchema("items" to Schema.FieldType.array(Schema.FieldType.STRING))
        val row = Row.withSchema(schema).addValue(listOf("a", "b")).build()
        val item = DynamoDBTypeMappings.rowToItem(row)
        val av = item["items"]!!
        assertEquals(AttributeValue.Type.L, av.type())
        assertEquals(2, av.l().size)
        assertEquals("a", av.l()[0].s())
        assertEquals("b", av.l()[1].s())
    }

    @Test
    fun `rowToItem - Map to M`() {
        val schema = buildSchema("kv" to Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.STRING))
        val row = Row.withSchema(schema).addValue(mapOf("k" to "v")).build()
        val item = DynamoDBTypeMappings.rowToItem(row)
        val av = item["kv"]!!
        assertEquals(AttributeValue.Type.M, av.type())
        assertEquals("v", av.m()["k"]!!.s())
    }

    @Test
    fun `rowToItem - BigDecimal to N`() {
        val schema = buildSchema("amount" to Schema.FieldType.DECIMAL)
        val row = Row.withSchema(schema).addValue(BigDecimal("999.99")).build()
        val item = DynamoDBTypeMappings.rowToItem(row)
        assertEquals("999.99", item["amount"]!!.n())
    }

    // =====================================================================
    // 4. Round-trip: rowToItem -> itemToRow
    // =====================================================================

    @Test
    fun `round-trip - String`() {
        val schema = buildSchema("name" to Schema.FieldType.STRING)
        val original = Row.withSchema(schema).addValue("Alice").build()
        val item = DynamoDBTypeMappings.rowToItem(original)
        val restored = DynamoDBTypeMappings.itemToRow(item, schema)
        assertEquals(original.getString("name"), restored.getString("name"))
    }

    @Test
    fun `round-trip - Boolean`() {
        val schema = buildSchema("flag" to Schema.FieldType.BOOLEAN)
        val original = Row.withSchema(schema).addValue(true).build()
        val item = DynamoDBTypeMappings.rowToItem(original)
        val restored = DynamoDBTypeMappings.itemToRow(item, schema)
        assertEquals(original.getBoolean("flag"), restored.getBoolean("flag"))
    }

    @Test
    fun `round-trip - null`() {
        val schema = buildSchema("opt" to Schema.FieldType.STRING)
        val original = Row.withSchema(schema).addValue(null).build()
        val item = DynamoDBTypeMappings.rowToItem(original)
        val restored = DynamoDBTypeMappings.itemToRow(item, schema)
        assertNull(restored.getString("opt"))
    }

    @Test
    fun `round-trip - ByteArray`() {
        val schema = buildSchema("blob" to Schema.FieldType.BYTES)
        val bytes = byteArrayOf(0xDE.toByte(), 0xAD.toByte(), 0xBE.toByte(), 0xEF.toByte())
        val original = Row.withSchema(schema).addValue(bytes).build()
        val item = DynamoDBTypeMappings.rowToItem(original)
        val restored = DynamoDBTypeMappings.itemToRow(item, schema)
        assertContentEquals(bytes, restored.getBytes("blob"))
    }

    @Test
    fun `round-trip - BigDecimal`() {
        val schema = buildSchema("amount" to Schema.FieldType.DECIMAL)
        val original = Row.withSchema(schema).addValue(BigDecimal("12345.6789")).build()
        val item = DynamoDBTypeMappings.rowToItem(original)
        val restored = DynamoDBTypeMappings.itemToRow(item, schema)
        assertEquals(original.getValue<BigDecimal>("amount"), restored.getValue<BigDecimal>("amount"))
    }

    @Test
    fun `round-trip - List`() {
        val schema = buildSchema("tags" to Schema.FieldType.array(Schema.FieldType.STRING))
        val original = Row.withSchema(schema).addValue(listOf("x", "y", "z")).build()
        val item = DynamoDBTypeMappings.rowToItem(original)
        val restored = DynamoDBTypeMappings.itemToRow(item, schema)
        assertEquals(listOf("x", "y", "z"), restored.getValue<List<String>>("tags"))
    }

    @Test
    fun `round-trip - Map`() {
        val schema = buildSchema("meta" to Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.STRING))
        val original = Row.withSchema(schema).addValue(mapOf("a" to "1", "b" to "2")).build()
        val item = DynamoDBTypeMappings.rowToItem(original)
        val restored = DynamoDBTypeMappings.itemToRow(item, schema)
        assertEquals(mapOf("a" to "1", "b" to "2"), restored.getValue<Map<String, String>>("meta"))
    }

    @Test
    fun `round-trip - multiple fields`() {
        // Use DECIMAL for numbers since DynamoDB stores N as strings
        val schema = buildSchema(
            "id" to Schema.FieldType.STRING,
            "count" to Schema.FieldType.DECIMAL,
            "active" to Schema.FieldType.BOOLEAN,
        )
        val original = Row.withSchema(schema)
            .addValue("abc")
            .addValue(java.math.BigDecimal("100"))
            .addValue(false)
            .build()
        val item = DynamoDBTypeMappings.rowToItem(original)
        val restored = DynamoDBTypeMappings.itemToRow(item, schema)
        assertEquals("abc", restored.getString("id"))
        assertEquals(java.math.BigDecimal("100"), restored.getValue("count"))
        assertEquals(false, restored.getBoolean("active"))
    }

    @Test
    fun `round-trip - empty list`() {
        val schema = buildSchema("items" to Schema.FieldType.array(Schema.FieldType.STRING))
        val original = Row.withSchema(schema).addValue(emptyList<String>()).build()
        val item = DynamoDBTypeMappings.rowToItem(original)
        val restored = DynamoDBTypeMappings.itemToRow(item, schema)
        assertEquals(emptyList<String>(), restored.getValue<List<String>>("items"))
    }

    @Test
    fun `round-trip - empty map`() {
        val schema = buildSchema("kv" to Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.STRING))
        val original = Row.withSchema(schema).addValue(emptyMap<String, String>()).build()
        val item = DynamoDBTypeMappings.rowToItem(original)
        val restored = DynamoDBTypeMappings.itemToRow(item, schema)
        assertEquals(emptyMap<String, String>(), restored.getValue<Map<String, String>>("kv"))
    }

    // =====================================================================
    // 5. toAttributeValue (public single-parameter overload)
    // =====================================================================

    @Test
    fun `toAttributeValue - null produces nul attribute`() {
        val av = DynamoDBTypeMappings.toAttributeValue(null)
        assertEquals(AttributeValue.Type.NUL, av.type())
        assertTrue(av.nul() == true)
    }

    @Test
    fun `toAttributeValue - String produces S attribute`() {
        val av = DynamoDBTypeMappings.toAttributeValue("hello")
        assertEquals(AttributeValue.Type.S, av.type())
        assertEquals("hello", av.s())
    }

    @Test
    fun `toAttributeValue - Int produces N attribute`() {
        val av = DynamoDBTypeMappings.toAttributeValue(42)
        assertEquals(AttributeValue.Type.N, av.type())
        assertEquals("42", av.n())
    }

    @Test
    fun `toAttributeValue - Long produces N attribute`() {
        val av = DynamoDBTypeMappings.toAttributeValue(123456789L)
        assertEquals(AttributeValue.Type.N, av.type())
        assertEquals("123456789", av.n())
    }

    @Test
    fun `toAttributeValue - Double produces N attribute`() {
        val av = DynamoDBTypeMappings.toAttributeValue(2.71828)
        assertEquals(AttributeValue.Type.N, av.type())
        assertEquals("2.71828", av.n())
    }

    @Test
    fun `toAttributeValue - Float produces N attribute`() {
        val av = DynamoDBTypeMappings.toAttributeValue(1.5f)
        assertEquals(AttributeValue.Type.N, av.type())
        assertNotNull(av.n())
    }

    @Test
    fun `toAttributeValue - BigDecimal produces N attribute`() {
        val av = DynamoDBTypeMappings.toAttributeValue(BigDecimal("123.456"))
        assertEquals(AttributeValue.Type.N, av.type())
        assertEquals("123.456", av.n())
    }

    @Test
    fun `toAttributeValue - Boolean true produces BOOL attribute`() {
        val av = DynamoDBTypeMappings.toAttributeValue(true)
        assertEquals(AttributeValue.Type.BOOL, av.type())
        assertTrue(av.bool())
    }

    @Test
    fun `toAttributeValue - Boolean false produces BOOL attribute`() {
        val av = DynamoDBTypeMappings.toAttributeValue(false)
        assertEquals(AttributeValue.Type.BOOL, av.type())
        assertFalse(av.bool())
    }

    @Test
    fun `toAttributeValue - ByteArray produces B attribute`() {
        val bytes = byteArrayOf(0x01, 0x02, 0x03)
        val av = DynamoDBTypeMappings.toAttributeValue(bytes)
        assertEquals(AttributeValue.Type.B, av.type())
        assertContentEquals(bytes, av.b().asByteArray())
    }

    @Test
    fun `toAttributeValue - SdkBytes produces B attribute`() {
        val sdkBytes = SdkBytes.fromByteArray(byteArrayOf(0xFF.toByte()))
        val av = DynamoDBTypeMappings.toAttributeValue(sdkBytes)
        assertEquals(AttributeValue.Type.B, av.type())
        assertContentEquals(byteArrayOf(0xFF.toByte()), av.b().asByteArray())
    }

    @Test
    fun `toAttributeValue - ByteBuffer produces B attribute`() {
        val buf = java.nio.ByteBuffer.wrap(byteArrayOf(0x0A, 0x0B))
        val av = DynamoDBTypeMappings.toAttributeValue(buf)
        assertEquals(AttributeValue.Type.B, av.type())
        assertContentEquals(byteArrayOf(0x0A, 0x0B), av.b().asByteArray())
    }

    @Test
    fun `toAttributeValue - empty List produces L attribute`() {
        val av = DynamoDBTypeMappings.toAttributeValue(emptyList<String>())
        assertEquals(AttributeValue.Type.L, av.type())
        assertEquals(0, av.l().size)
    }

    @Test
    fun `toAttributeValue - non-empty List produces L attribute`() {
        val av = DynamoDBTypeMappings.toAttributeValue(listOf("a", "b"))
        assertEquals(AttributeValue.Type.L, av.type())
        assertEquals(2, av.l().size)
        assertEquals("a", av.l()[0].s())
        assertEquals("b", av.l()[1].s())
    }

    @Test
    fun `toAttributeValue - Array produces L attribute`() {
        val av = DynamoDBTypeMappings.toAttributeValue(arrayOf("x", "y"))
        assertEquals(AttributeValue.Type.L, av.type())
        assertEquals(2, av.l().size)
    }

    @Test
    fun `toAttributeValue - Map produces M attribute`() {
        val av = DynamoDBTypeMappings.toAttributeValue(mapOf("k1" to "v1"))
        assertEquals(AttributeValue.Type.M, av.type())
        assertEquals("v1", av.m()["k1"]!!.s())
    }

    @Test
    fun `toAttributeValue - empty Map produces empty M attribute`() {
        val av = DynamoDBTypeMappings.toAttributeValue(emptyMap<String, String>())
        assertEquals(AttributeValue.Type.M, av.type())
        assertEquals(0, av.m().size)
    }

    @Test
    fun `toAttributeValue - empty ByteArray produces B with empty bytes`() {
        val av = DynamoDBTypeMappings.toAttributeValue(ByteArray(0))
        assertEquals(AttributeValue.Type.B, av.type())
        assertEquals(0, av.b().asByteArray().size)
    }

    @Test
    fun `toAttributeValue - list elements are converted to S`() {
        // Lists infer as ARRAY<STRING>, so each element is converted via toAttributeValue(value, STRING)
        val av = DynamoDBTypeMappings.toAttributeValue(listOf(1, 2, 3))
        assertEquals(AttributeValue.Type.L, av.type())
        assertEquals("1", av.l()[0].s())
        assertEquals("2", av.l()[1].s())
        assertEquals("3", av.l()[2].s())
    }

    @Test
    fun `toAttributeValue - map values are converted to S`() {
        val av = DynamoDBTypeMappings.toAttributeValue(mapOf("count" to 42))
        assertEquals(AttributeValue.Type.M, av.type())
        assertEquals("42", av.m()["count"]!!.s())
    }

    // =====================================================================
    // 6. deriveSchema + itemToRow end-to-end
    // =====================================================================

    @Test
    fun `deriveSchema then itemToRow - full pipeline`() {
        val items = listOf(
            mapOf(
                "id" to AttributeValue.builder().s("1").build(),
                "value" to AttributeValue.builder().n("100").build(),
                "active" to AttributeValue.builder().bool(true).build(),
            ),
        )
        val schema = DynamoDBTypeMappings.deriveSchema(items)
        val row = DynamoDBTypeMappings.itemToRow(items[0], schema)
        assertEquals("1", row.getString("id"))
        assertEquals(BigDecimal("100"), row.getValue<BigDecimal>("value"))
        assertEquals(true, row.getBoolean("active"))
    }

    @Test
    fun `deriveSchema then round-trip`() {
        val item = mapOf(
            "name" to AttributeValue.builder().s("test").build(),
            "count" to AttributeValue.builder().n("7").build(),
        )
        val schema = DynamoDBTypeMappings.deriveSchema(listOf(item))
        val row = DynamoDBTypeMappings.itemToRow(item, schema)
        val restoredItem = DynamoDBTypeMappings.rowToItem(row)
        val restoredRow = DynamoDBTypeMappings.itemToRow(restoredItem, schema)
        assertEquals(row.getString("name"), restoredRow.getString("name"))
        assertEquals(row.getValue<BigDecimal>("count"), restoredRow.getValue<BigDecimal>("count"))
    }
}
