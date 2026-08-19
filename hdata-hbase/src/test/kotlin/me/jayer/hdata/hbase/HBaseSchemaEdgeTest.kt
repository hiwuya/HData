package me.jayer.hdata.hbase

import org.apache.beam.sdk.schemas.Schema
import org.apache.hadoop.hbase.util.Bytes
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertContentEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * `HBaseSchema` 里此前零覆盖、却最可能"静默出错"的纯逻辑：
 *  - [buildReadSchema] 对重名列 / 列名撞 rowkey 的报错（不拦的话 Beam 会在更隐蔽处抛看不懂的错）；
 *  - [RowkeyFormat.encode] 对各种非法 rowkey 的报错与正常编码（二进制 rowkey 必须用 bytes，否则 UTF-8 破坏）；
 *  - [HBaseType.of] 大小写不敏感、[HBaseType] 的编解码边界。
 *
 * @author wuya
 */
class HBaseSchemaEdgeTest {

    @Test
    fun `不同列族下同名列会撞车并明确指出`() {
        val cols = listOf(
            HBaseColumn.parse("tag:STRING", "cf"),
            HBaseColumn.parse("ext:tag:STRING", "cf"),
        )
        val error = assertFailsWith<IllegalArgumentException> { buildReadSchema("rowkey", RowkeyFormat.STRING, cols) }
        assertTrue("tag" in error.message!!)
    }

    @Test
    fun `schema_fields 的列名与 rowkey 撞了报错`() {
        val cols = listOf(HBaseColumn.parse("rowkey:STRING", "cf"))
        val error = assertFailsWith<IllegalArgumentException> { buildReadSchema("rowkey", RowkeyFormat.STRING, cols) }
        assertTrue("rowkey" in error.message!!)
    }

    @Test
    fun `rowkey 为 null 时报错`() {
        val error = assertFailsWith<IllegalStateException> { RowkeyFormat.STRING.encode(null) }
        assertTrue("rowkey" in error.message!!)
    }

    @Test
    fun `bytes 格式遇到非字节数组 rowkey 时报错而不是拿 toString 顶上`() {
        val error = assertFailsWith<IllegalArgumentException> { RowkeyFormat.BYTES.encode("r1") }
        assertTrue("bytes" in error.message!!)
    }

    @Test
    fun `string 格式遇到非字符串 rowkey 时报错`() {
        val error = assertFailsWith<IllegalArgumentException> { RowkeyFormat.STRING.encode(123L) }
        assertTrue("string" in error.message!!)
    }

    @Test
    fun `string 格式编码走 UTF-8`() {
        assertContentEquals(Bytes.toBytes("r1"), RowkeyFormat.STRING.encode("r1"))
    }

    @Test
    fun `bytes 格式原样返回字节数组`() {
        val b = byteArrayOf(0x00, 0x01, 0xFF.toByte())
        assertContentEquals(b, RowkeyFormat.BYTES.encode(b) as ByteArray)
    }

    @Test
    fun `HBaseType_of 大小写不敏感`() {
        assertEquals(HBaseType.STRING, HBaseType.of("string"))
        assertEquals(HBaseType.INT32, HBaseType.of("INT32"))
    }

    @Test
    fun `BYTES 列 decode 原样返回字节`() {
        val bytes = Bytes.toBytes("hello")
        assertContentEquals(
            bytes,
            HBaseType.BYTES.decode(HBaseColumn.parse("blob:BYTES", "cf"), bytes) as ByteArray,
        )
    }

    @Test
    fun `INT32 列可以接受 Long 这种更宽的数字类型`() {
        assertContentEquals(
            Bytes.toBytes(7),
            HBaseType.INT32.encode(HBaseColumn.parse("qty:INT32", "cf"), 7L) as ByteArray,
        )
    }
}
