package me.jayer.hdata.iceberg

import me.jayer.hdata.iceberg.internal.fieldTypeOf
import me.jayer.hdata.iceberg.internal.parseSchemaFields
import me.jayer.hdata.iceberg.internal.recordToRow
import me.jayer.hdata.iceberg.internal.rowToRecord
import me.jayer.hdata.iceberg.internal.schemaOf
import me.jayer.hdata.iceberg.internal.validateReadableSchema
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

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

    @Test
    fun `未知字段类型报错`() {
        // 写/读两端的 validate 都走 fieldTypeOf，未知类型必须当场报，不能静默落盘成错列
        assertFailsWith<IllegalArgumentException> { fieldTypeOf("WEIRD") }
    }

    @Test
    fun `schema_fields 空字段名报错`() {
        assertFailsWith<IllegalArgumentException> { parseSchemaFields(listOf(" :STRING")) }
    }

    @Test
    fun `整数转换拒绝小数与越界而不是截断`() {
        val declared = listOf("age:INT32")
        val source = Schema.builder().addDoubleField("age").build()
        val fractional = Row.withSchema(source).addValue(1.5).build()
        assertFailsWith<ArithmeticException> {
            rowToRecord(schemaOf(declared), fractional, parseSchemaFields(declared))
        }

        val longSource = Schema.builder().addInt64Field("age").build()
        val overflow = Row.withSchema(longSource).addValue(2_147_483_648L).build()
        assertFailsWith<ArithmeticException> {
            rowToRecord(schemaOf(declared), overflow, parseSchemaFields(declared))
        }
    }

    @Test
    fun `读取声明必须与真实表字段名和类型一致`() {
        val actual = schemaOf(listOf("id:INT64", "name:STRING"))

        validateReadableSchema(actual, parseSchemaFields(listOf("id:INT64")), "db.t")
        assertFailsWith<IllegalArgumentException> {
            validateReadableSchema(actual, parseSchemaFields(listOf("missing:STRING")), "db.t")
        }
        assertFailsWith<IllegalArgumentException> {
            validateReadableSchema(actual, parseSchemaFields(listOf("id:STRING")), "db.t")
        }
    }
}
