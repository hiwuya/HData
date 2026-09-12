package me.jayer.hdata.hive.type

import me.jayer.hdata.core.type.FieldTypes
import org.apache.beam.sdk.schemas.Schema
import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

/**
 * Parsing of Hive type strings. The focus is **nesting**: the type in the metastore is one whole string, and splitting on commas
 * cannot resolve `map<string,array<struct<a:int,b:string>>>`.
 *
 * @author wuya
 */
class HiveTypesTest {

    @Test
    fun `parses basic types`() {
        assertEquals(FieldTypes.BYTE, HiveTypes.parse("tinyint"))
        assertEquals(FieldTypes.INT16, HiveTypes.parse("smallint"))
        assertEquals(FieldTypes.INT32, HiveTypes.parse("int"))
        assertEquals(FieldTypes.INT64, HiveTypes.parse("bigint"))
        assertEquals(FieldTypes.FLOAT, HiveTypes.parse("float"))
        assertEquals(FieldTypes.DOUBLE, HiveTypes.parse("double"))
        assertEquals(FieldTypes.BOOLEAN, HiveTypes.parse("boolean"))
        assertEquals(FieldTypes.STRING, HiveTypes.parse("string"))
        assertEquals(FieldTypes.BYTES, HiveTypes.parse("binary"))
        assertEquals(FieldTypes.DATE, HiveTypes.parse("date"))
    }

    @Test
    fun `a parameterized type keeps only the type itself`() {
        assertEquals(FieldTypes.STRING, HiveTypes.parse("varchar(255)"))
        assertEquals(FieldTypes.STRING, HiveTypes.parse("char(3)"))
        assertEquals(FieldTypes.DECIMAL, HiveTypes.parse("decimal(10,2)"))
        assertEquals(FieldTypes.DECIMAL, HiveTypes.parse("decimal"))
        assertEquals(10 to 2, HiveTypes.decimalPrecisionAndScale("decimal(10,2)"))
        // A decimal without parameters is the pre-Hive 0.12 spelling, equivalent to decimal(10,0)
        assertEquals(10 to 0, HiveTypes.decimalPrecisionAndScale("decimal"))
    }

    @Test
    fun `timestamp and timestamp with local time zone are two different types`() {
        // Hive's timestamp carries no time zone, it is a wall-clock time
        assertEquals(FieldTypes.DATETIME, HiveTypes.parse("timestamp"))
        assertEquals(FieldTypes.TIMESTAMP, HiveTypes.parse("timestamp with local time zone"))
    }

    @Test
    fun `parses a nested type`() {
        val type = HiveTypes.parse("map<string,array<struct<a:int,b:string>>>")
        assertEquals(Schema.TypeName.MAP, type.typeName)
        assertEquals(FieldTypes.STRING, type.mapKeyType)

        val arrayType = type.mapValueType!!
        assertEquals(Schema.TypeName.ARRAY, arrayType.typeName)

        val rowType = arrayType.collectionElementType!!
        assertEquals(Schema.TypeName.ROW, rowType.typeName)
        assertEquals(listOf("a", "b"), rowType.rowSchema!!.fieldNames)
        assertEquals(FieldTypes.INT32, rowType.rowSchema!!.getField("a").type.withNullable(false))
    }

    @Test
    fun `an unrecognized type throws directly, no fallback`() {
        assertFailsWith<IllegalArgumentException> { HiveTypes.parse("uniontype<int,string>") }
        assertFailsWith<IllegalArgumentException> { HiveTypes.parse("blob") }
        assertFailsWith<IllegalArgumentException> { HiveTypes.parse("struct<a:int") }
        // Trailing content must be reported too, so that "int garbage" is not read as an int
        assertFailsWith<IllegalArgumentException> { HiveTypes.parse("int garbage") }
    }

    @Test
    fun `Beam type converts back to a Hive type`() {
        assertEquals("int", HiveTypes.toHiveType(FieldTypes.INT32))
        assertEquals("timestamp", HiveTypes.toHiveType(FieldTypes.DATETIME))
        assertEquals("timestamp with local time zone", HiveTypes.toHiveType(FieldTypes.TIMESTAMP))
        assertEquals(
            "array<string>",
            HiveTypes.toHiveType(Schema.FieldType.array(FieldTypes.STRING)),
        )
    }

    @Test
    fun `a partition literal is restored per the column type`() {
        assertEquals(LocalDate.of(2024, 1, 1), HiveValues.fromPartitionLiteral("2024-01-01", FieldTypes.DATE))
        assertEquals(1, HiveValues.fromPartitionLiteral("1", FieldTypes.INT32))
        // __HIVE_DEFAULT_PARTITION__ is restored to null
        assertNull(HiveValues.fromPartitionLiteral("__HIVE_DEFAULT_PARTITION__", FieldTypes.STRING))
    }

    @Test
    fun `a value that fails to parse becomes null instead of throwing`() {
        // Consistent with Hive's LazySimpleSerDe: the dirty column becomes NULL instead of failing the whole job
        assertNull(HiveValues.parseString("abc", FieldTypes.INT32))
        assertNull(HiveValues.parseString("2024-13-45", FieldTypes.DATE))
        assertEquals(BigDecimal("1.50"), HiveValues.parseString("1.50", FieldTypes.DECIMAL))
    }

    @Test
    fun `Hive's timestamp literal separates date and time with a space`() {
        assertEquals(
            LocalDateTime.of(2024, 1, 1, 10, 30, 0),
            HiveValues.parseString("2024-01-01 10:30:00", FieldTypes.DATETIME),
        )
    }

    @Test
    fun `the write side aligns by column name, not by index`() {
        val target = Schema.builder()
            .addNullableField("id", FieldTypes.INT64)
            .addNullableField("name", FieldTypes.STRING)
            .build()
        // The upstream field order is the reverse of the target table's, and there is one extra column
        val source = Schema.builder()
            .addNullableField("name", FieldTypes.STRING)
            .addNullableField("extra", FieldTypes.STRING)
            .addNullableField("id", FieldTypes.INT32)
            .build()
        val row = org.apache.beam.sdk.values.Row.withSchema(source)
            .addValues("Alice", "ignored", 7)
            .build()

        val aligned = HiveValues.align(row, target)
        assertEquals(7L, aligned.getValue<Long>("id"))
        assertEquals("Alice", aligned.getValue<String>("name"))
    }

    @Test
    fun `narrowing a numeric value out of range throws directly, no silent truncation`() {
        assertFailsWith<IllegalArgumentException> { HiveValues.coerce(300L, FieldTypes.BYTE) }
        assertEquals(100.toByte(), HiveValues.coerce(100L, FieldTypes.BYTE))
    }
}
