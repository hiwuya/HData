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
 * Edge cases of [HBaseRowCodec] that were previously untouched:
 *  - a null column should be skipped on write while the rest are written normally (missing or spurious
 *    columns both corrupt the HBase row);
 *  - a bytes-format rowkey is written by its raw bytes, not degraded to UTF-8;
 *  - a BYTES column reads back as the raw bytes;
 *  - `scan_cache_blocks: true` actually turns on block caching (a config option must really take effect,
 *    otherwise one full-table scan would evict the hot blocks of an online workload).
 *
 * @author wuya
 */
class HBaseRowCodecEdgeTest {

    private fun codec(fields: List<String>, rowkeyFormat: String = "string", family: String = "cf") =
        HBaseRowCodec.of("rowkey", rowkeyFormat, fields, family)

    @Test
    fun `a null column is skipped while the rest are written normally`() {
        val schema = Schema.builder()
            .addStringField("rowkey")
            .addNullableStringField("name")
            .addNullableInt32Field("age")
            .build()
        val row = Row.withSchema(schema).addValue("r1").addValue(null).addValue(30).build()

        val put = codec(listOf("name:STRING", "age:INT32")).toPut(row)

        val qualifiers = put.getFamilyCellMap()[Bytes.toBytes("cf")]
            ?.map { Bytes.toString(CellUtil.cloneQualifier(it)) } ?: emptyList()
        assertTrue("name" !in qualifiers, "a null column should not be written: $qualifiers")
        assertTrue("age" in qualifiers)
    }

    @Test
    fun `a bytes-format rowkey is written by its raw bytes`() {
        val schema = Schema.builder().addByteArrayField("rowkey").addNullableStringField("name").build()
        val rk = byteArrayOf(0x00, 0xFF.toByte(), 0x01)
        val row = Row.withSchema(schema).addValue(rk).addValue("x").build()

        val put = codec(listOf("name:STRING"), rowkeyFormat = "bytes").toPut(row)

        assertContentEquals(rk, put.row)
    }

    @Test
    fun `a BYTES column reads back as the raw bytes`() {
        val cell = CellUtil.createCell(
            Bytes.toBytes("r1"), Bytes.toBytes("cf"), Bytes.toBytes("blob"),
            1L, org.apache.hadoop.hbase.Cell.Type.Put.code, Bytes.toBytes("hello"),
        )
        val result = Result.create(listOf(cell))

        val row = codec(listOf("blob:BYTES")).toRow(result)

        assertContentEquals(Bytes.toBytes("hello"), row.getBytes("blob"))
    }

    @Test
    fun `scan_cache_blocks true actually turns on block caching`() {
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
