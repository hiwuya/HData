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
 * Beam Row 与 HBase Result / Put 的互转。
 *
 * 不需要 HBase 集群：[Result] 可以直接用 [Cell] 拼出来。
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
    fun `读出的行以 rowkey 开头，之后按 schema_fields 顺序`() {
        val row = codec().toRow(
            result(
                Bytes.toBytes("r1"),
                Triple("cf", "name", Bytes.toBytes("张三")),
                Triple("cf", "age", Bytes.toBytes(30)),
            )
        )

        assertEquals("r1", row.getString("rowkey"))
        assertEquals("张三", row.getString("name"))
        assertEquals(30, row.getInt32("age"))
    }

    @Test
    fun `缺失的列读成 null`() {
        val row = codec().toRow(result(Bytes.toBytes("r1"), Triple("cf", "name", Bytes.toBytes("张三"))))

        assertNull(row.getInt32("age"))
    }

    @Test
    fun `family 冒号 qualifier 冒号 type 可以跨列族`() {
        // 重构前只认 qualifier:type，等于把"所有列都在同一列族"写死进了实现
        val codec = codec(fields = listOf("name:STRING", "ext:tag:STRING"))

        val row = codec.toRow(
            result(
                Bytes.toBytes("r1"),
                Triple("cf", "name", Bytes.toBytes("张三")),
                Triple("ext", "tag", Bytes.toBytes("vip")),
            )
        )

        assertEquals("张三", row.getString("name"))
        assertEquals("vip", row.getString("tag"))
    }

    @Test
    fun `定长类型遇到宽度对不上的单元格时报错，而不是静默截断`() {
        // Bytes.toInt 对超过 4 字节的输入只取前 4 字节：数据是错的但作业照常成功，
        // 这是最难发现的一类问题
        val eightBytes = Bytes.toBytes(1234567890123L)

        val error = assertFailsWith<IllegalArgumentException> {
            codec().toRow(result(Bytes.toBytes("r1"), Triple("cf", "age", eightBytes)))
        }

        assertTrue("age" in error.message!! && "8" in error.message!!)
    }

    @Test
    fun `二进制 rowkey 用 bytes 格式原样保留`() {
        // 这串字节不是合法 UTF-8，按 string 读会被替换字符破坏
        val rowkey = byteArrayOf(0x00, 0xFF.toByte(), 0x01)
        val codec = codec(rowkeyFormat = "bytes")

        val row = codec.toRow(result(rowkey, Triple("cf", "name", Bytes.toBytes("x"))))

        assertContentEquals(rowkey, row.getBytes("rowkey"))
        assertEquals(Schema.TypeName.BYTES, codec.schema.getField("rowkey").type.typeName)
    }

    @Test
    fun `写入时 rowkey 按声明的格式编码`() {
        val schema = Schema.builder().addStringField("rowkey").addNullableStringField("name").build()
        val row = Row.withSchema(schema).addValue("r1").addValue("张三").build()

        val put = codec().toPut(row)

        assertContentEquals(Bytes.toBytes("r1"), put.row)
    }

    @Test
    fun `rowkey 类型与 rowkey_format 不符时报错而不是拿 toString 顶上`() {
        // 重构前是 Bytes.toBytes(rowkey.toString())：Long 被写成十进制字符串、
        // ByteArray 被写成 "[B@1a2b3c"，读回来永远对不上
        val schema = Schema.builder().addInt64Field("rowkey").addNullableStringField("name").build()
        val row = Row.withSchema(schema).addValue(1L).addValue("张三").build()

        val error = assertFailsWith<IllegalArgumentException> { codec().toPut(row) }

        assertTrue("rowkey_format" in error.message!! || "STRING" in error.message!!)
    }

    @Test
    fun `所有列都是 null 时报错，HBase 不接受空 Put`() {
        val schema = Schema.builder().addStringField("rowkey").addNullableStringField("name").build()
        val row = Row.withSchema(schema).addValue("r1").addValue(null).build()

        val error = assertFailsWith<IllegalArgumentException> { codec(fields = listOf("name:STRING")).toPut(row) }

        assertTrue("r1" in error.message!!)
    }

    @Test
    fun `缺少 rowkey 字段时报错并列出现有字段`() {
        val schema = Schema.builder().addNullableStringField("name").build()
        val row = Row.withSchema(schema).addValue("张三").build()

        val error = assertFailsWith<IllegalArgumentException> { codec().toPut(row) }

        assertTrue("rowkey" in error.message!! && "name" in error.message!!)
    }

    @Test
    fun `字段类型与声明不符时报错指出是哪一列`() {
        val schema = Schema.builder().addStringField("rowkey").addNullableStringField("age").build()
        val row = Row.withSchema(schema).addValue("r1").addValue("三十").build()

        val error = assertFailsWith<IllegalArgumentException> { codec(fields = listOf("age:INT32")).toPut(row) }

        assertTrue("age" in error.message!!)
    }

    @Test
    fun `codec 可以跟着 DoFn 一起序列化下发`() {
        // 里面缓存了列族/列名的字节数组，捕获了不可序列化的东西会在提交时才炸
        SerializableUtils.ensureSerializable(codec(fields = listOf("name:STRING", "ext:tag:INT64")))
    }

    @Test
    fun `schema_fields 条目格式不对时报错`() {
        assertFailsWith<IllegalArgumentException> { HBaseColumn.parse("a:b:c:d", "cf") }
        assertFailsWith<IllegalArgumentException> { HBaseColumn.parse(":STRING", "cf") }
        assertFailsWith<IllegalArgumentException> { HBaseColumn.parse("name:WAT", "cf") }
    }

    @Test
    fun `不写类型时默认按 STRING 处理`() {
        assertEquals(HBaseType.STRING, HBaseColumn.parse("name", "cf").type)
        assertEquals("cf", HBaseColumn.parse("name", "cf").family)
    }
}
