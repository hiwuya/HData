package me.jayer.hdata.hbase

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.util.SerializableUtils
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Conversion between Beam Row and HBase Result / Put.
 *
 * No HBase cluster is needed; [Result] values can be assembled directly from [Cell] instances.
 *
 * @author wuya
 */
class HBaseRowCodecTest {

    private fun codec(
        fields: List<String> = listOf("name:STRING", "age:INT32"),
        rowkeyFormat: String = "string",
        family: String = "cf",
    ) = HBaseRowCodec.of("rowkey", rowkeyFormat, fields, family)

    private fun result(rowkey: ByteArray, vararg cells: Triple<String, String, ByteArray>): Result =
        Result.create(
            cells.map { (family, qualifier, value) ->
                CellUtil.createCell(rowkey, Bytes.toBytes(family), Bytes.toBytes(qualifier), 1L, Cell.Type.Put.code, value)
            }.sortedWith(CellComparatorAdapter),
        )

    private object CellComparatorAdapter : Comparator<Cell> {
        override fun compare(a: Cell, b: Cell): Int =
            org.apache.hadoop.hbase.CellComparator.getInstance().compare(a, b)
    }

    @Test
    fun `read rows start with rowkey and follow schema_fields order`() {
        val row = codec().toRow(
            result(
                Bytes.toBytes("r1"),
                Triple("cf", "name", Bytes.toBytes("Alice")),
                Triple("cf", "age", Bytes.toBytes(30)),
            )
        )

        assertEquals("r1", row.getString("rowkey"))
        assertEquals("Alice", row.getString("name"))
        assertEquals(30, row.getInt32("age"))
    }

    @Test
    fun `missing columns are read as null`() {
        val row = codec().toRow(result(Bytes.toBytes("r1"), Triple("cf", "name", Bytes.toBytes("Alice"))))

        assertNull(row.getInt32("age"))
    }

    @Test
    fun `family qualifier type syntax supports multiple families`() {
        val codec = codec(fields = listOf("name:STRING", "ext:tag:STRING"))

        val row = codec.toRow(
            result(
                Bytes.toBytes("r1"),
                Triple("cf", "name", Bytes.toBytes("Alice")),
                Triple("ext", "tag", Bytes.toBytes("vip")),
            )
        )

        assertEquals("Alice", row.getString("name"))
        assertEquals("vip", row.getString("tag"))
    }

    @Test
    fun `fixed-width types reject cells with the wrong width`() {
        // Bytes.toInt silently truncates oversized input, so invalid data could otherwise appear successful.
        val eightBytes = Bytes.toBytes(1234567890123L)

        val error = assertFailsWith<IllegalArgumentException> {
            codec().toRow(result(Bytes.toBytes("r1"), Triple("cf", "age", eightBytes)))
        }

        assertTrue("age" in error.message!! && "8" in error.message!!)
    }

    @Test
    fun `binary rowkeys are preserved with bytes format`() {
        val rowkey = byteArrayOf(0x00, 0xFF.toByte(), 0x01)
        val codec = codec(rowkeyFormat = "bytes")

        val row = codec.toRow(result(rowkey, Triple("cf", "name", Bytes.toBytes("x"))))

        assertContentEquals(rowkey, row.getBytes("rowkey"))
        assertEquals(Schema.TypeName.BYTES, codec.schema.getField("rowkey").type.typeName)
    }

    @Test
    fun `write rowkeys use the declared format`() {
        val schema = Schema.builder().addStringField("rowkey").addNullableStringField("name").build()
        val row = Row.withSchema(schema).addValue("r1").addValue("Alice").build()

        val put = codec(fields = listOf("name:STRING")).toPut(row)

        assertContentEquals(Bytes.toBytes("r1"), put.row)
    }

    @Test
    fun `rowkey type mismatch is rejected instead of using toString`() {
        val schema = Schema.builder().addInt64Field("rowkey").addNullableStringField("name").build()
        val row = Row.withSchema(schema).addValue(1L).addValue("Alice").build()

        val error = assertFailsWith<IllegalArgumentException> { codec().toPut(row) }

        assertTrue("rowkey_format" in error.message!! || "STRING" in error.message!!)
    }

    @Test
    fun `all-null columns are rejected because HBase does not accept empty puts`() {
        val schema = Schema.builder().addStringField("rowkey").addNullableStringField("name").build()
        val row = Row.withSchema(schema).addValue("r1").addValue(null).build()

        val error = assertFailsWith<IllegalArgumentException> { codec(fields = listOf("name:STRING")).toPut(row) }

        assertTrue("r1" in error.message!!)
    }

    @Test
    fun `missing rowkey field reports available fields`() {
        val schema = Schema.builder().addNullableStringField("name").build()
        val row = Row.withSchema(schema).addValue("Alice").build()

        val error = assertFailsWith<IllegalArgumentException> { codec().toPut(row) }

        assertTrue("rowkey" in error.message!! && "name" in error.message!!)
    }

    @Test
    fun `missing declared columns are rejected instead of partial writes`() {
        val schema = Schema.builder().addStringField("rowkey").addInt32Field("age").build()
        val row = Row.withSchema(schema).addValue("r1").addValue(30).build()

        val error = assertFailsWith<IllegalArgumentException> {
            codec(fields = listOf("name:STRING", "age:INT32")).toPut(row)
        }

        assertTrue("name" in error.message!! && "age" in error.message!!)
    }

    @Test
    fun `field type mismatches identify the column`() {
        val schema = Schema.builder().addStringField("rowkey").addNullableStringField("age").build()
        val row = Row.withSchema(schema).addValue("r1").addValue("thirty").build()

        val error = assertFailsWith<IllegalArgumentException> { codec(fields = listOf("age:INT32")).toPut(row) }

        assertTrue("age" in error.message!!)
    }

    @Test
    fun `fractional and overflowing integers are rejected`() {
        val schema = Schema.builder().addStringField("rowkey").addDoubleField("age").build()
        val row = Row.withSchema(schema).addValue("r1").addValue(1.5).build()
        assertFailsWith<IllegalArgumentException> { codec(fields = listOf("age:INT32")).toPut(row) }

        val longSchema = Schema.builder().addStringField("rowkey").addInt64Field("age").build()
        val overflow = Row.withSchema(longSchema).addValue("r1").addValue(2_147_483_648L).build()
        assertFailsWith<IllegalArgumentException> { codec(fields = listOf("age:INT32")).toPut(overflow) }
    }

    @Test
    fun `codec is serializable with a DoFn`() {
        // Cached family and qualifier bytes must remain serializable for job submission.
        SerializableUtils.ensureSerializable(codec(fields = listOf("name:STRING", "ext:tag:INT64")))
    }

    @Test
    fun `invalid schema_fields syntax is rejected`() {
        assertFailsWith<IllegalArgumentException> { HBaseColumn.parse("a:b:c:d", "cf") }
        assertFailsWith<IllegalArgumentException> { HBaseColumn.parse(":STRING", "cf") }
        assertFailsWith<IllegalArgumentException> { HBaseColumn.parse("name:WAT", "cf") }
    }

    @Test
    fun `missing type defaults to STRING`() {
        assertEquals(HBaseType.STRING, HBaseColumn.parse("name", "cf").type)
        assertEquals("cf", HBaseColumn.parse("name", "cf").family)
    }
}
