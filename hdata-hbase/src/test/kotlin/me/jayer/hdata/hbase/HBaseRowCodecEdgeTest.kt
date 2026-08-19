package me.jayer.hdata.hbase

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * [HBaseRowCodec] 此前没被触达的边界：
 *  - 写入时某列是 null 应当跳过它、但其余列照常写（漏写/多写都会让 HBase 行数据错）；
 *  - bytes 格式的 rowkey 按字节写入，不能退化成 UTF-8；
 *  - BYTES 列读出原样字节；
 *  - `scan_cache_blocks` 配置为真时确实打开块缓存（配置项必须真的生效，否则一条全表扫描就把在线业务的热点挤出去了）。
 *
 * @author wuya
 */
class HBaseRowCodecEdgeTest {

    private fun codec(fields: List<String>, rowkeyFormat: String = "string", family: String = "cf") =
        HBaseRowCodec.of("rowkey", rowkeyFormat, fields, family)

    @Test
    fun `某列是 null 时跳过它但其余列照写`() {
        val schema = Schema.builder()
            .addStringField("rowkey")
            .addNullableStringField("name")
            .addNullableInt32Field("age")
            .build()
        val row = Row.withSchema(schema).addValue("r1").addValue(null).addValue(30).build()

        val put = codec(listOf("name:STRING", "age:INT32")).toPut(row)

        val qualifiers = put.getFamilyCellMap()[Bytes.toBytes("cf")]
            ?.map { Bytes.toString(CellUtil.cloneQualifier(it)) } ?: emptyList()
        assertTrue("name" !in qualifiers, "null 列不应被写入: $qualifiers")
        assertTrue("age" in qualifiers)
    }

    @Test
    fun `bytes 格式的 rowkey 按字节写入`() {
        val schema = Schema.builder().addByteArrayField("rowkey").addNullableStringField("name").build()
        val rk = byteArrayOf(0x00, 0xFF.toByte(), 0x01)
        val row = Row.withSchema(schema).addValue(rk).addValue("x").build()

        val put = codec(listOf("name:STRING"), rowkeyFormat = "bytes").toPut(row)

        assertContentEquals(rk, put.row)
    }

    @Test
    fun `BYTES 列读出原样字节`() {
        val cell = CellUtil.createCell(
            Bytes.toBytes("r1"), Bytes.toBytes("cf"), Bytes.toBytes("blob"),
            1L, org.apache.hadoop.hbase.Cell.Type.Put.code, Bytes.toBytes("hello"),
        )
        val result = Result.create(listOf(cell))

        val row = codec(listOf("blob:BYTES")).toRow(result)

        assertContentEquals(Bytes.toBytes("hello"), row.getBytes("blob"))
    }

    @Test
    fun `scan_cache_blocks 配置为 true 时确实打开块缓存`() {
        val config = HBaseReadConfig(
            zookeeperQuorum = "localhost:2181",
            table = "t",
            schemaFields = listOf("name:STRING"),
            scanCacheBlocks = true,
        )
        val scan = config.scan()
        assertTrue(scan.cacheBlocks)
    }
}
