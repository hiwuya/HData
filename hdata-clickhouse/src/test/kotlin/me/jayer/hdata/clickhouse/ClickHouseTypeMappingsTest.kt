package me.jayer.hdata.clickhouse

import me.jayer.hdata.clickhouse.internal.ClickHouseTypeMappings
import org.apache.beam.sdk.schemas.Schema
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ClickHouseTypeMappingsTest {

    @Test
    fun `maps basic types`() {
        assertEquals(Schema.FieldType.STRING, ClickHouseTypeMappings.toBeamType("String"))
        assertEquals(Schema.FieldType.INT32, ClickHouseTypeMappings.toBeamType("Int32"))
        assertEquals(Schema.FieldType.INT64, ClickHouseTypeMappings.toBeamType("Int64"))
        assertEquals(Schema.FieldType.FLOAT, ClickHouseTypeMappings.toBeamType("Float32"))
        assertEquals(Schema.FieldType.DOUBLE, ClickHouseTypeMappings.toBeamType("Float64"))
        assertEquals(Schema.FieldType.BOOLEAN, ClickHouseTypeMappings.toBeamType("Bool"))
        assertEquals(Schema.FieldType.DATETIME, ClickHouseTypeMappings.toBeamType("DateTime"))
        assertEquals(Schema.FieldType.BYTES, ClickHouseTypeMappings.toBeamType("Binary"))
    }

    @Test
    fun `strips Nullable wrapper`() {
        assertEquals(Schema.FieldType.INT32, ClickHouseTypeMappings.toBeamType("Nullable(Int32)"))
        assertTrue(ClickHouseTypeMappings.isNullable("Nullable(Int32)"))
        assertFalse(ClickHouseTypeMappings.isNullable("Int32"))
    }

    @Test
    fun `strips LowCardinality wrapper`() {
        assertEquals(Schema.FieldType.STRING, ClickHouseTypeMappings.toBeamType("LowCardinality(String)"))
        assertEquals(Schema.FieldType.STRING, ClickHouseTypeMappings.toBeamType("LowCardinality(Nullable(String))"))
    }

    @Test
    fun `strips parameters from Decimal`() {
        assertEquals(Schema.FieldType.DECIMAL, ClickHouseTypeMappings.toBeamType("Decimal(18, 4)"))
    }

    @Test
    fun `maps UInt types`() {
        assertEquals(Schema.FieldType.INT16, ClickHouseTypeMappings.toBeamType("UInt8"))
        assertEquals(Schema.FieldType.INT32, ClickHouseTypeMappings.toBeamType("UInt16"))
        assertEquals(Schema.FieldType.INT64, ClickHouseTypeMappings.toBeamType("UInt32"))
        assertEquals(Schema.FieldType.DECIMAL, ClickHouseTypeMappings.toBeamType("UInt64"))
    }

    @Test
    fun `falls back to STRING for unknown types`() {
        assertEquals(Schema.FieldType.STRING, ClickHouseTypeMappings.toBeamType("SomeCustomType"))
        assertEquals(Schema.FieldType.STRING, ClickHouseTypeMappings.toBeamType("Enum8('a'=1)"))
    }

    @Test
    fun `is case insensitive`() {
        assertEquals(Schema.FieldType.INT32, ClickHouseTypeMappings.toBeamType("int32"))
        assertEquals(Schema.FieldType.INT32, ClickHouseTypeMappings.toBeamType("INT32"))
        assertTrue(ClickHouseTypeMappings.isNullable("nullable(int32)"))
    }
}
