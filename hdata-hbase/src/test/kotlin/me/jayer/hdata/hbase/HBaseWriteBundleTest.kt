package me.jayer.hdata.hbase

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.testing.CollectingFinishBundleContext
import me.jayer.hdata.hbase.transform.HBaseWriteFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Table
import org.joda.time.Duration
import org.joda.time.Instant
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.io.IOException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * `HBaseWriteFn` 的**攒批与死信**行为测试。
 *
 * 重点在 `Table.batch` 的**逐行结果**：`results[i]` 是 `Throwable` 表示这一行失败、
 * null 表示这一行根本没被尝试、其余表示成功。重构前的实现看的是"整批有没有抛异常"，
 * 于是一条脏数据会把整批都记成失败，而且死信里 `element` 是 null、时间戳是现编的。
 *
 * 用 mock 的 [Table]，不需要 HBase 集群。
 *
 * @author wuya
 */
class HBaseWriteBundleTest {

    private val codec = HBaseRowCodec.of("rowkey", "string", listOf("name:STRING", "age:INT32"), "cf")
    private val errorSchema = ErrorSchemas.of(codec.schema)

    /** 每次 `batch` 提交进来的批次。 */
    private val submitted = mutableListOf<List<Put>>()

    /** 下一次 `batch` 往 `results` 里填的内容：Throwable 记失败、null 记没尝试、其余记成功。 */
    private var batchResults: List<Any?> = emptyList()

    /** 设成非 null 后，下一次 `batch` 在填完 results 之后再抛它。 */
    private var batchError: Throwable? = null

    private fun row(rowkey: String, name: String? = "张三", age: Int? = 30): Row =
        Row.withSchema(codec.schema).addValue(rowkey).addValue(name).addValue(age).build()

    private fun window(): BoundedWindow = IntervalWindow(Instant(0), Duration.millis(10))

    private fun writeFn(batchSize: Int = 2, deadLetter: Boolean = true): HBaseWriteFn {
        val table = mock<Table>()
        whenever(table.batch(anyOrNull<List<Put>>(), anyOrNull<Array<Any?>>())).thenAnswer { inv ->
            (inv.getArgument(0) as? List<*>)?.let { @Suppress("UNCHECKED_CAST") submitted.add(it as List<Put>) }
            val results = inv.getArgument(1) as Array<Any?>?
            if (results != null) {
                batchResults.forEachIndexed { i, value -> if (i < results.size) results[i] = value }
            }
            batchError?.let { throw it }
            null
        }
        val fn = HBaseWriteFn(
            HBaseWriteConfig(
                zookeeperQuorum = "localhost:2181",
                table = "t",
                rowkeyField = "rowkey",
                family = "cf",
                schemaFields = listOf("name:STRING", "age:INT32"),
                batchSize = batchSize,
            ),
            codec,
            errorSchema,
            deadLetter,
            "WriteToHBase",
        )
        fn.testTable = table
        fn.setup()
        return fn
    }

    private fun feed(fn: HBaseWriteFn, vararg rows: Row) =
        rows.forEach { fn.processElement(it, Instant(1), window(), PaneInfo.NO_FIRING) }

    /** 跑完一个 bundle，返回收集到的死信输出。 */
    private fun finishBundleOf(fn: HBaseWriteFn): CollectingFinishBundleContext<Row, Row> {
        val context = CollectingFinishBundleContext<Row, Row>()
        fn.finishBundle(context.context())
        return context
    }

    private fun elementOf(failureRow: Row): Row = failureRow.getValue(ErrorSchemas.ELEMENT)

    @Test
    fun `攒够 batch_size 才提交一次，而且一次提交整批`() {
        val fn = writeFn(batchSize = 2)

        feed(fn, row("r1"))
        assertEquals(0, submitted.size, "还没攒够一批，不该提前提交")

        feed(fn, row("r2"))

        assertEquals(1, submitted.size, "攒够 batch_size 应该只发一次 batch")
        assertEquals(2, submitted[0].size, "一次要把整批都交出去，而不是逐条 put")
    }

    @Test
    fun `finishBundle 把最后不足一批的行提交掉`() {
        val fn = writeFn(batchSize = 100)

        feed(fn, row("r1"))
        fn.finishBundle(CollectingFinishBundleContext<Row, Row>().context())

        assertEquals(1, submitted.size)
        assertEquals(1, submitted[0].size)
    }

    @Test
    fun `batch 的逐行结果里只有 Throwable 那行进死信`() {
        val fn = writeFn(batchSize = 3)
        batchResults = listOf(Any(), IOException("NotServingRegion"), Any())

        feed(fn, row("r1"), row("r2"), row("r3"))
        val context = finishBundleOf(fn)

        assertEquals(1, context.outputs.size, "只有 results 里是 Throwable 的那行失败")
        assertEquals("r2", elementOf(context.outputs[0]).getString("rowkey"))
        val message = context.outputs[0].getString(ErrorSchemas.ERROR_MESSAGE)
        assertTrue(message!!.contains("NotServingRegion"), "死信要带真实的 HBase 异常，实际: $message")
    }

    @Test
    fun `整批提交就挂掉时全部行进死信，错误是真实异常`() {
        val fn = writeFn(batchSize = 2)
        batchError = IOException("connection reset")

        feed(fn, row("r1"), row("r2"))
        val context = finishBundleOf(fn)

        // results 全 null 表示一行都没轮到，此时用整批的异常，而不是编一个"未返回写入结果"
        assertEquals(listOf("r1", "r2"), context.outputs.map { elementOf(it).getString("rowkey") })
        assertTrue(context.outputs.all { it.getString(ErrorSchemas.ERROR_MESSAGE)!!.contains("connection reset") })
    }

    @Test
    fun `死信带的是原始行自己的时间戳与窗口`() {
        val fn = writeFn(batchSize = 1)
        batchError = IOException("region offline")
        val stamp = Instant(4242)
        val win = IntervalWindow(Instant(100), Duration.millis(100))

        fn.processElement(row("r1"), stamp, win, PaneInfo.NO_FIRING)
        val context = finishBundleOf(fn)

        assertEquals(1, context.outputs.size)
        assertEquals(stamp, context.timestamps[0], "死信必须沿用原始行的时间戳，现编 Instant.now() 没法重放")
        assertEquals(win, context.windows[0], "死信必须沿用原始行的窗口，写死 GlobalWindow 在窗口化 pipeline 里会抛异常")
    }

    @Test
    fun `成功后队列清空，重跑同一个 bundle 不会重复提交`() {
        val fn = writeFn(batchSize = 10)

        feed(fn, row("r1"), row("r2"))
        fn.finishBundle(CollectingFinishBundleContext<Row, Row>().context())
        fn.startBundle()
        fn.finishBundle(CollectingFinishBundleContext<Row, Row>().context())

        assertEquals(1, submitted.size, "批提交完必须清空，否则 runner 重试同一个 bundle 会把旧行再写一遍")
    }

    @Test
    fun `没开死信时写入失败直接抛异常`() {
        val fn = writeFn(batchSize = 1, deadLetter = false)
        batchError = IOException("boom")

        assertFailsWith<IOException> { feed(fn, row("r1")) }
    }

    @Test
    fun `列全为 null 的行在构 Put 时就被拒，不会带到 batch`() {
        val fn = writeFn(batchSize = 1)

        feed(fn, row("r1", name = null, age = null))
        val context = finishBundleOf(fn)

        assertEquals(0, submitted.size, "空 Put 不该发给 HBase")
        assertEquals(1, context.outputs.size)
        assertTrue(context.outputs[0].getString(ErrorSchemas.ERROR_MESSAGE)!!.contains("r1"))
    }
}
