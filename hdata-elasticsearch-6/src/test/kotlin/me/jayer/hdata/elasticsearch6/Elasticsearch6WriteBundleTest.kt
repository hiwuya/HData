package me.jayer.hdata.elasticsearch6

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.testing.CollectingFinishBundleContext
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.Row
import org.elasticsearch.action.bulk.BulkItemResponse
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestHighLevelClient
import org.joda.time.Duration
import org.joda.time.Instant
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.io.IOException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * `Elasticsearch6WriteFn` 的**攒批与死信**行为测试。
 *
 * `RestHighLevelClient` 的 `bulk` 是 final 方法，所以这个模块用 inline mock maker
 * （见 pom 里的 `mockito-inline`）；仍然不连真实 ES。
 *
 * @author wuya
 */
class Elasticsearch6WriteBundleTest {

    private val fields = parseSchemaFields(listOf("id:STRING", "name:STRING"))
    private val schema = buildSchema(fields)
    private val errorSchema = ErrorSchemas.of(schema)

    /** 每次 `bulk` 提交进来的请求条数。 */
    private val submitted = mutableListOf<Int>()

    /** 下一次 `bulk` 返回的结果；null 表示直接抛 [bulkError]。 */
    private var bulkResponse: BulkResponse? = null
    private var bulkError: Throwable? = null

    private fun row(id: String, name: String? = "张三"): Row =
        Row.withSchema(schema).addValue(id).addValue(name).build()

    private fun window(): BoundedWindow = IntervalWindow(Instant(0), Duration.millis(10))

    private fun writeFn(batchSize: Int = 2, deadLetter: Boolean = true): Elasticsearch6WriteFn {
        val client = mock<RestHighLevelClient>()
        whenever(client.bulk(anyOrNull<BulkRequest>(), anyOrNull<RequestOptions>())).thenAnswer { inv ->
            submitted.add((inv.getArgument(0) as? BulkRequest)?.numberOfActions() ?: 0)
            bulkError?.let { throw it } ?: bulkResponse
        }
        val fn = Elasticsearch6WriteFn(
            nodes = listOf("http://localhost:9200"),
            index = "orders",
            username = "",
            password = "",
            fields = fields,
            batchSize = batchSize,
            inputSchema = schema,
            errorSchema = errorSchema,
            deadLetter = deadLetter,
            transformName = "WriteToElasticsearch6",
        )
        fn.clientFactory = Es6ClientFactory { client }
        fn.setup()
        return fn
    }

    private fun feed(fn: Elasticsearch6WriteFn, vararg rows: Row) =
        rows.forEach { fn.processElement(it, Instant(1), window(), PaneInfo.NO_FIRING) }

    /** 跑完一个 bundle，返回收集到的死信输出。 */
    private fun finishBundleOf(fn: Elasticsearch6WriteFn): CollectingFinishBundleContext<Row, Row> {
        val context = CollectingFinishBundleContext<Row, Row>()
        fn.finishBundle(context.context())
        return context
    }

    private fun elementOf(failureRow: Row): Row = failureRow.getValue(ErrorSchemas.ELEMENT)

    private fun bulkOf(vararg failedIndexes: Int, size: Int): BulkResponse {
        val items = (0 until size).map { i ->
            val item = mock<BulkItemResponse>()
            whenever(item.isFailed).doReturn(i in failedIndexes)
            whenever(item.failureMessage).doReturn("index [$i] rejected")
            item
        }
        return BulkResponse(items.toTypedArray(), 0L)
    }

    @Test
    fun `攒够 batch_size 才提交一次，而且一次提交整批`() {
        val fn = writeFn(batchSize = 2)
        bulkResponse = bulkOf(size = 2)

        feed(fn, row("a1"))
        assertEquals(0, submitted.size, "还没攒够一批，不该提前提交")

        feed(fn, row("a2"))

        assertEquals(listOf(2), submitted, "攒够 batch_size 应该只发一次 bulkRequest，一次交整批")
    }

    @Test
    fun `finishBundle 把最后不足一批的行提交掉`() {
        val fn = writeFn(batchSize = 100)
        bulkResponse = bulkOf(size = 1)

        feed(fn, row("a1"))
        fn.finishBundle(CollectingFinishBundleContext<Row, Row>().context())

        assertEquals(listOf(1), submitted)
    }

    @Test
    fun `只有失败的那几条进死信，成功的照常写进去`() {
        val fn = writeFn(batchSize = 3)
        bulkResponse = bulkOf(failedIndexes = intArrayOf(1), size = 3)

        feed(fn, row("a1"), row("a2"), row("a3"))
        val context = finishBundleOf(fn)

        assertEquals(1, context.outputs.size)
        assertEquals("a2", elementOf(context.outputs[0]).getString("id"))
        assertTrue(context.outputs[0].getString(ErrorSchemas.ERROR_MESSAGE)!!.contains("index [1] rejected"))
    }

    @Test
    fun `整批连接异常时全部行进死信，错误是真实异常`() {
        val fn = writeFn(batchSize = 2)
        bulkError = IOException("connection reset")

        feed(fn, row("a1"), row("a2"))
        val context = finishBundleOf(fn)

        assertEquals(listOf("a1", "a2"), context.outputs.map { elementOf(it).getString("id") })
        assertTrue(context.outputs.all { it.getString(ErrorSchemas.ERROR_MESSAGE)!!.contains("connection reset") })
    }

    @Test
    fun `死信带的是原始行自己的时间戳与窗口`() {
        val fn = writeFn(batchSize = 1)
        bulkError = IOException("cluster blocked")
        val stamp = Instant(4242)
        val win = IntervalWindow(Instant(100), Duration.millis(100))

        fn.processElement(row("a1"), stamp, win, PaneInfo.NO_FIRING)
        val context = finishBundleOf(fn)

        assertEquals(1, context.outputs.size)
        assertEquals(stamp, context.timestamps[0], "死信必须沿用原始行的时间戳，现编 Instant.now() 没法重放")
        assertEquals(win, context.windows[0], "死信必须沿用原始行的窗口，写死 GlobalWindow 在窗口化 pipeline 里会抛异常")
    }

    @Test
    fun `没开死信时写入失败直接抛异常`() {
        val fn = writeFn(batchSize = 1, deadLetter = false)
        bulkError = IOException("boom")

        assertFailsWith<IOException> { feed(fn, row("a1")) }
    }

    @Test
    fun `没配 schema_fields 时按 document 列写，与读端的产出对得上`() {
        // 读端不声明 schema_fields 时产出的列就叫 document；写端曾经找的是 value，
        // 于是"读出来再写回去"这个文档里承诺的用法一行也走不通
        val client = mock<RestHighLevelClient>()
        // bulkOf 里也是 whenever，必须先算完再进 stubbing，否则报 UnfinishedStubbing
        val ok = bulkOf(size = 1)
        whenever(client.bulk(anyOrNull<BulkRequest>(), anyOrNull<RequestOptions>())).doReturn(ok)
        val fn = Elasticsearch6WriteFn(
            nodes = listOf("http://localhost:9200"),
            index = "orders",
            username = "",
            password = "",
            fields = emptyList(),
            batchSize = 1,
            inputSchema = DOCUMENT_SCHEMA,
            errorSchema = ErrorSchemas.of(DOCUMENT_SCHEMA),
            deadLetter = true,
            transformName = "WriteToElasticsearch6",
        )
        fn.clientFactory = Es6ClientFactory { client }
        fn.setup()

        val document = Row.withSchema(DOCUMENT_SCHEMA).addValue("""{"id":"a1"}""").build()
        fn.processElement(document, Instant(1), window(), PaneInfo.NO_FIRING)
        val context = finishBundleOf(fn)

        assertEquals(0, context.outputs.size, "读端产出的 document 列必须能直接写回")
    }
}
