package me.jayer.hdata.debezium.internal

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class DebeziumRecordsTest {

    @Test
    fun `Envelope 转 Row`() {
        val afterSchema = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build()
        val schema = SchemaBuilder.struct()
            .field("op", Schema.STRING_SCHEMA)
            .field("before", Schema.OPTIONAL_STRING_SCHEMA)
            .field("after", afterSchema)
            .field("source", Schema.OPTIONAL_STRING_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
            .build()
        val value = Struct(schema)
            .put("op", "c")
            .put("before", null)
            .put("after", Struct(afterSchema).put("id", 1L))
            .put("source", null)
            .put("ts_ms", 123L)
        val record = SourceRecord(emptyMap<String, Any?>(), emptyMap<String, Any?>(), "topic", schema, value)
        val row = DebeziumRecords.toRow(record)!!
        assertEquals("c", row.getString("op"))
        assertEquals(123L, row.getInt64("ts_ms"))
        assertEquals("""{"id":1}""", row.getString("after"))
        assertNull(row.getValue("before"))
    }

    @Test
    fun `非 Envelope 兜底成 r 行`() {
        val schema = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build()
        val value = Struct(schema).put("id", 7L)
        val record = SourceRecord(emptyMap<String, Any?>(), emptyMap<String, Any?>(), "topic", schema, value)
        val row = DebeziumRecords.toRow(record)!!
        assertEquals("r", row.getString("op"))
        assertEquals("""{"id":7}""", row.getString("after"))
    }

    @Test
    fun `空 value 跳过`() {
        val record = SourceRecord(emptyMap<String, Any?>(), emptyMap<String, Any?>(), "topic", Schema.STRING_SCHEMA, null)
        assertEquals(null, DebeziumRecords.toRow(record))
    }
}
