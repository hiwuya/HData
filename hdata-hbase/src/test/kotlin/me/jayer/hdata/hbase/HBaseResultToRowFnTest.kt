package me.jayer.hdata.hbase

import me.jayer.hdata.core.testing.CollectingOutputReceiver
import me.jayer.hdata.hbase.transform.HBaseResultToRowFn
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * `HBaseResultToRowFn` 的测试：把 `HBaseIO.readAll()` 吐出的 [Result] 转成带 schema 的 [Row]。
 *
 * 读端唯一自己写的一节，重构前它和连接池混在一起，连个 Result 都造不出来就不可能被测到。
 *
 * @author wuya
 */
class HBaseResultToRowFnTest {

    private val codec = HBaseRowCodec.of("rowkey", "string", listOf("name:STRING", "age:INT32"), "cf")

    private fun result(rowkey: String, vararg cells: Triple<String, String, ByteArray>): Result =
        Result.create(
            cells.map { (family, qualifier, value) ->
                CellUtil.createCell(
                    Bytes.toBytes(rowkey), Bytes.toBytes(family), Bytes.toBytes(qualifier),
                    1L, Cell.Type.Put.code, value,
                )
            }.sortedWith(org.apache.hadoop.hbase.CellComparator.getInstance()),
        )

    @Test
    fun `把 Result 转成带 schema 的行`() {
        val fn = HBaseResultToRowFn(codec)
        val receiver = CollectingOutputReceiver<Row>()

        fn.processElement(
            result(
                "r1",
                Triple("cf", "name", Bytes.toBytes("张三")),
                Triple("cf", "age", Bytes.toBytes(30)),
            ),
            receiver,
        )

        assertEquals(1, receiver.outputs.size)
        val row = receiver.outputs[0]
        assertEquals("r1", row.getString("rowkey"))
        assertEquals("张三", row.getString("name"))
        assertEquals(30, row.getInt32("age"))
    }
}
