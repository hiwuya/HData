package me.jayer.hdata.cassandra

import com.datastax.oss.driver.api.core.type.DataTypes
import com.datastax.oss.driver.internal.core.type.DefaultCustomType
import me.jayer.hdata.cassandra.internal.CassandraTypeMappings
import org.apache.beam.sdk.schemas.Schema
import kotlin.test.Test
import kotlin.test.assertEquals

class CassandraTypeMappingsTest {

    // ── Scalar types ──────────────────────────────────────────────

    @Test
    fun `maps TEXT and ASCII to STRING`() {
        assertEquals(Schema.FieldType.STRING, CassandraTypeMappings.toBeamType(DataTypes.TEXT))
        assertEquals(Schema.FieldType.STRING, CassandraTypeMappings.toBeamType(DataTypes.ASCII))
    }

    @Test
    fun `maps INT to INT32`() {
        assertEquals(Schema.FieldType.INT32, CassandraTypeMappings.toBeamType(DataTypes.INT))
    }

    @Test
    fun `maps BIGINT and COUNTER to INT64`() {
        assertEquals(Schema.FieldType.INT64, CassandraTypeMappings.toBeamType(DataTypes.BIGINT))
        assertEquals(Schema.FieldType.INT64, CassandraTypeMappings.toBeamType(DataTypes.COUNTER))
    }

    @Test
    fun `maps SMALLINT to INT16`() {
        assertEquals(Schema.FieldType.INT16, CassandraTypeMappings.toBeamType(DataTypes.SMALLINT))
    }

    @Test
    fun `maps TINYINT to BYTE`() {
        assertEquals(Schema.FieldType.BYTE, CassandraTypeMappings.toBeamType(DataTypes.TINYINT))
    }

    @Test
    fun `maps FLOAT to FLOAT`() {
        assertEquals(Schema.FieldType.FLOAT, CassandraTypeMappings.toBeamType(DataTypes.FLOAT))
    }

    @Test
    fun `maps DOUBLE to DOUBLE`() {
        assertEquals(Schema.FieldType.DOUBLE, CassandraTypeMappings.toBeamType(DataTypes.DOUBLE))
    }

    @Test
    fun `maps BOOLEAN to BOOLEAN`() {
        assertEquals(Schema.FieldType.BOOLEAN, CassandraTypeMappings.toBeamType(DataTypes.BOOLEAN))
    }

    @Test
    fun `maps DECIMAL and VARINT to DECIMAL`() {
        assertEquals(Schema.FieldType.DECIMAL, CassandraTypeMappings.toBeamType(DataTypes.DECIMAL))
        assertEquals(Schema.FieldType.DECIMAL, CassandraTypeMappings.toBeamType(DataTypes.VARINT))
    }

    @Test
    fun `maps UUID and TIMEUUID to STRING`() {
        assertEquals(Schema.FieldType.STRING, CassandraTypeMappings.toBeamType(DataTypes.UUID))
        assertEquals(Schema.FieldType.STRING, CassandraTypeMappings.toBeamType(DataTypes.TIMEUUID))
    }

    @Test
    fun `maps BLOB to BYTES`() {
        assertEquals(Schema.FieldType.BYTES, CassandraTypeMappings.toBeamType(DataTypes.BLOB))
    }

    @Test
    fun `maps DATE and TIMESTAMP to DATETIME`() {
        assertEquals(Schema.FieldType.DATETIME, CassandraTypeMappings.toBeamType(DataTypes.DATE))
        assertEquals(Schema.FieldType.DATETIME, CassandraTypeMappings.toBeamType(DataTypes.TIMESTAMP))
    }

    @Test
    fun `maps TIME to STRING`() {
        assertEquals(Schema.FieldType.STRING, CassandraTypeMappings.toBeamType(DataTypes.TIME))
    }

    @Test
    fun `maps INET and DURATION to STRING`() {
        assertEquals(Schema.FieldType.STRING, CassandraTypeMappings.toBeamType(DataTypes.INET))
        assertEquals(Schema.FieldType.STRING, CassandraTypeMappings.toBeamType(DataTypes.DURATION))
    }

    // ── Collection types ──────────────────────────────────────────

    @Test
    fun `maps ListType to ARRAY`() {
        val fieldType = CassandraTypeMappings.toBeamType(DataTypes.listOf(DataTypes.TEXT))
        assertEquals(Schema.FieldType.array(Schema.FieldType.STRING), fieldType)
    }

    @Test
    fun `maps SetType to ARRAY`() {
        val fieldType = CassandraTypeMappings.toBeamType(DataTypes.setOf(DataTypes.INT))
        assertEquals(Schema.FieldType.array(Schema.FieldType.INT32), fieldType)
    }

    @Test
    fun `maps MapType to MAP`() {
        val fieldType = CassandraTypeMappings.toBeamType(DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT))
        assertEquals(Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.INT32), fieldType)
    }

    @Test
    fun `maps nested collection types`() {
        // list<list<bigint>> -> ARRAY<ARRAY<INT64>>
        val innerList = DataTypes.listOf(DataTypes.BIGINT)
        val outerList = CassandraTypeMappings.toBeamType(DataTypes.listOf(innerList))
        assertEquals(
            Schema.FieldType.array(Schema.FieldType.array(Schema.FieldType.INT64)),
            outerList
        )

        // map<text, set<double>> -> MAP<STRING, ARRAY<DOUBLE>>
        val innerSet = DataTypes.setOf(DataTypes.DOUBLE)
        val mapType = CassandraTypeMappings.toBeamType(DataTypes.mapOf(DataTypes.TEXT, innerSet))
        assertEquals(
            Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.array(Schema.FieldType.DOUBLE)),
            mapType
        )
    }

    // ── Tuple type ────────────────────────────────────────────────

    @Test
    fun `maps TupleType to ARRAY of first component type`() {
        val tupleType = DataTypes.tupleOf(DataTypes.TEXT, DataTypes.INT, DataTypes.BOOLEAN)
        val fieldType = CassandraTypeMappings.toBeamType(tupleType)
        assertEquals(Schema.FieldType.array(Schema.FieldType.STRING), fieldType)
    }

    @Test
    fun `maps empty TupleType to ARRAY of STRING`() {
        val tupleType = DataTypes.tupleOf()
        val fieldType = CassandraTypeMappings.toBeamType(tupleType)
        assertEquals(Schema.FieldType.array(Schema.FieldType.STRING), fieldType)
    }

    // ── Fallback ──────────────────────────────────────────────────

    @Test
    fun `falls back to STRING for unknown types`() {
        // DefaultCustomType is not matched by any when branch
        val unknown = DefaultCustomType("com.example.UnknownType")
        assertEquals(Schema.FieldType.STRING, CassandraTypeMappings.toBeamType(unknown))
    }
}
