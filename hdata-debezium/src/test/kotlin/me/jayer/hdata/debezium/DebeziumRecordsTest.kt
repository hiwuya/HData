package me.jayer.hdata.debezium.internal

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.junit.jupiter.api.Test
import java.util.Base64
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

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

    @Test
    fun `BYTES 字段 base64 编码`() {
        // Binary columns like those from Iceberg/HBase are ByteArrays in a SourceRecord; base64 was once not applied,
        // and stuffing them straight into JSON would blow up at runtime (or write out garbage), yet no case touched it.
        val payload = byteArrayOf(0, 1, 2, 127, -1, -128)
        val schema = SchemaBuilder.struct().field("payload", Schema.BYTES_SCHEMA).build()
        val value = Struct(schema).put("payload", payload)
        val record = SourceRecord(emptyMap<String, Any?>(), emptyMap<String, Any?>(), "topic", schema, value)
        val after = DebeziumRecords.toRow(record)!!.getString("after")!!
        val expected = Base64.getEncoder().encodeToString(payload)
        assertTrue(after.contains(expected), "after should contain the base64-encoded binary: $after")
    }

    @Test
    fun `ByteBuffer 字段同样 base64 编码`() {
        val payload = byteArrayOf(3, 4, 5)
        val schema = SchemaBuilder.struct().field("payload", Schema.BYTES_SCHEMA).build()
        val value = Struct(schema).put("payload", java.nio.ByteBuffer.wrap(payload))
        val record = SourceRecord(emptyMap<String, Any?>(), emptyMap<String, Any?>(), "topic", schema, value)
        val after = DebeziumRecords.toRow(record)!!.getString("after")!!
        assertTrue(after.contains(Base64.getEncoder().encodeToString(payload)))
    }

    @Test
    fun `Map 字段转 JSON 对象`() {
        val mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build()
        val schema = SchemaBuilder.struct().field("m", mapSchema).build()
        val value = Struct(schema).put("m", mapOf("a" to 1, "b" to 2))
        val record = SourceRecord(emptyMap<String, Any?>(), emptyMap<String, Any?>(), "topic", schema, value)
        val after = DebeziumRecords.toRow(record)!!.getString("after")!!
        assertTrue(after.contains("\"a\":1"), after)
        assertTrue(after.contains("\"b\":2"), after)
    }

    @Test
    fun `Array 字段转 JSON 数组`() {
        val arrSchema = SchemaBuilder.array(Schema.INT32_SCHEMA).build()
        val schema = SchemaBuilder.struct().field("arr", arrSchema).build()
        val value = Struct(schema).put("arr", listOf(1, 2, 3))
        val record = SourceRecord(emptyMap<String, Any?>(), emptyMap<String, Any?>(), "topic", schema, value)
        val after = DebeziumRecords.toRow(record)!!.getString("after")!!
        assertTrue(after.contains("[1,2,3]"), after)
    }

    @Test
    fun `二进制主键 base64 进 key 字段`() {
        val schema = SchemaBuilder.struct().field("x", Schema.INT64_SCHEMA).build()
        val value = Struct(schema).put("x", 9L)
        val bytes = byteArrayOf(1, 2, 3)
        val record = SourceRecord(
            emptyMap<String, Any?>(), emptyMap<String, Any?>(), "topic",
            Schema.BYTES_SCHEMA, bytes, schema, value,
        )
        val row = DebeziumRecords.toRow(record)!!
        assertEquals("\"${Base64.getEncoder().encodeToString(bytes)}\"", row.getString("key")!!)
    }

    @Test
    fun `缺 ts_ms 取值时 ts_ms 为 null`() {
        // A real Debezium Envelope always carries the five fields op/before/after/source/ts_ms; only ts_ms may be
        // unset — here we verify with an envelope that fills in only op/after and leaves ts_ms empty.
        val schema = SchemaBuilder.struct()
            .field("op", Schema.STRING_SCHEMA)
            .field("before", Schema.OPTIONAL_STRING_SCHEMA)
            .field("after", Schema.OPTIONAL_STRING_SCHEMA)
            .field("source", Schema.OPTIONAL_STRING_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
            .build()
        val value = Struct(schema).put("op", "c").put("after", "x")
        val record = SourceRecord(emptyMap<String, Any?>(), emptyMap<String, Any?>(), "topic", schema, value)
        val row = DebeziumRecords.toRow(record)!!
        assertNull(row.getValue("ts_ms"))
    }

    @Test
    fun `不同结构的两张表都映射到固定输出 schema`() {
        // The output schema is fixed (op/key/before/after/source/ts_ms), independent of the external table structure.
        // This is precisely the premise of "a single pipeline can capture multiple tables of differing structures":
        // one has only id, another has id+name, and both must land in the same fixed schema, otherwise downstream
        // cannot consume them.
        val s1 = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build()
        val s2 = SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA).field("name", Schema.STRING_SCHEMA).build()
        val r1 = SourceRecord(emptyMap<String, Any?>(), emptyMap<String, Any?>(), "t1", s1, Struct(s1).put("id", 1L))
        val r2 = SourceRecord(
            emptyMap<String, Any?>(), emptyMap<String, Any?>(), "t2", s2,
            Struct(s2).put("id", 2L).put("name", "x"),
        )
        val row1 = DebeziumRecords.toRow(r1)!!
        val row2 = DebeziumRecords.toRow(r2)!!
        assertEquals(DebeziumRecords.SCHEMA, row1.schema)
        assertEquals(DebeziumRecords.SCHEMA, row2.schema)
        assertEquals("r", row1.getString("op"))
        assertEquals("r", row2.getString("op"))
        assertEquals("""{"id":1}""", row1.getString("after"))
        assertEquals("""{"id":2,"name":"x"}""", row2.getString("after"))
    }
}
