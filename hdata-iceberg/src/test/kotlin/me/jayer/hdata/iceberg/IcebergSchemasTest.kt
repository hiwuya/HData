package me.jayer.hdata.iceberg

import me.jayer.hdata.iceberg.internal.parseSchemaFields
import me.jayer.hdata.iceberg.internal.recordToRow
import me.jayer.hdata.iceberg.internal.rowToRecord
import me.jayer.hdata.iceberg.internal.schemaOf
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import kotlin.test.Test
import kotlin.test.assertEquals

class IcebergSchemasTest {

    private val fields = listOf("id:INT64", "name:STRING", "age:INT32", "score:FLOAT64", "ok:BOOLEAN")
    private val beamSchema = Schema.builder()
        .addInt64Field("id").addStringField("name").addInt32Field("age").addDoubleField("score").addBooleanField("ok")
        .build()

    @Test
    fun `Beam 与 Iceberg schema 互转`() {
        val iceberg = schemaOf(fields)
        assertEquals(5, iceberg.columns().size)
        assertEquals("id", iceberg.columns()[0].name())
    }

    @Test
    fun `行与 Iceberg Record 互转保持一致`() {
        val row = Row.withSchema(beamSchema)
            .addValue(1L).addValue("alice").addValue(30).addValue(1.5).addValue(true).build()
        val parsed = parseSchemaFields(fields)
        val record = rowToRecord(schemaOf(fields), row, parsed)
        assertEquals(1L, record.getField("id"))
        assertEquals("alice", record.getField("name"))
        assertEquals(30, record.getField("age"))
        assertEquals(1.5, record.getField("score"))
        assertEquals(true, record.getField("ok"))
        val back = recordToRow(beamSchema, record, parsed)
        assertEquals(row, back)
    }
}
