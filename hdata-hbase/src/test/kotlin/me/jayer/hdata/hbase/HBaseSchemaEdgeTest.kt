package me.jayer.hdata.hbase

import org.apache.beam.sdk.schemas.Schema
import org.apache.hadoop.hbase.util.Bytes
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertContentEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Pure logic in `HBaseSchema` that previously had zero coverage yet was the most likely to fail silently:
 *  - [buildReadSchema] rejecting a duplicate column name / a column name colliding with rowkey (without this
 *    guard Beam throws an unreadable error somewhere much less obvious);
 *  - [RowkeyFormat.encode] rejecting various invalid rowkeys, and correct encoding for valid ones (a binary
 *    rowkey must use bytes, otherwise UTF-8 corrupts it);
 *  - [HBaseType.of] case-insensitivity, and [HBaseType] encode/decode edge cases.
 *
 * @author wuya
 */
class HBaseSchemaEdgeTest {

    @Test
    fun `same column name under different families collides and is called out explicitly`() {
        val cols = listOf(
            HBaseColumn.parse("tag:STRING", "cf"),
            HBaseColumn.parse("ext:tag:STRING", "cf"),
        )
        val error = assertFailsWith<IllegalArgumentException> { buildReadSchema("rowkey", RowkeyFormat.STRING, cols) }
        assertTrue("tag" in error.message!!)
    }

    @Test
    fun `a schema_fields column name colliding with rowkey errors`() {
        val cols = listOf(HBaseColumn.parse("rowkey:STRING", "cf"))
        val error = assertFailsWith<IllegalArgumentException> { buildReadSchema("rowkey", RowkeyFormat.STRING, cols) }
        assertTrue("rowkey" in error.message!!)
    }

    @Test
    fun `a null rowkey errors`() {
        val error = assertFailsWith<IllegalStateException> { RowkeyFormat.STRING.encode(null) }
        assertTrue("rowkey" in error.message!!)
    }

    @Test
    fun `bytes format errors on a non-byte-array rowkey instead of falling back to toString`() {
        val error = assertFailsWith<IllegalArgumentException> { RowkeyFormat.BYTES.encode("r1") }
        assertTrue("bytes" in error.message!!)
    }

    @Test
    fun `string format errors on a non-string rowkey`() {
        val error = assertFailsWith<IllegalArgumentException> { RowkeyFormat.STRING.encode(123L) }
        assertTrue("string" in error.message!!)
    }

    @Test
    fun `string format encodes as UTF-8`() {
        assertContentEquals(Bytes.toBytes("r1"), RowkeyFormat.STRING.encode("r1"))
    }

    @Test
    fun `bytes format returns the byte array as-is`() {
        val b = byteArrayOf(0x00, 0x01, 0xFF.toByte())
        assertContentEquals(b, RowkeyFormat.BYTES.encode(b) as ByteArray)
    }

    @Test
    fun `HBaseType_of is case-insensitive`() {
        assertEquals(HBaseType.STRING, HBaseType.of("string"))
        assertEquals(HBaseType.INT32, HBaseType.of("INT32"))
    }

    @Test
    fun `a BYTES column decodes to the raw bytes`() {
        val bytes = Bytes.toBytes("hello")
        assertContentEquals(
            bytes,
            HBaseType.BYTES.decode(HBaseColumn.parse("blob:BYTES", "cf"), bytes) as ByteArray,
        )
    }

    @Test
    fun `an INT32 column accepts a wider Long value`() {
        assertContentEquals(
            Bytes.toBytes(7),
            HBaseType.INT32.encode(HBaseColumn.parse("qty:INT32", "cf"), 7L) as ByteArray,
        )
    }
}
