package me.jayer.hdata.elasticsearch8

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch._types.ErrorCause
import co.elastic.clients.elasticsearch.core.BulkRequest
import co.elastic.clients.elasticsearch.core.BulkResponse
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem
import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.testing.CollectingFinishBundleContext
import me.jayer.hdata.elasticsearch8.transform.EsWriteFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.Row
import org.joda.time.Duration
import org.joda.time.Instant
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * `EsWriteFn` 的**攒批与死信**行为测试：攒够 `batch_size` 才发一次 bulk、
 * bulk 的逐条结果里只有失败那行进死信、整批异常时错误是真实的那个异常、
 * 死信沿用原始行的时间戳与窗口。
 *
 * 用 mock 的 [ElasticsearchClient]，不需要 ES 集群。
 *
 * @author wuya
 */
class EsWriteBundleTest {

    private val schema = buildSchema(listOf("id:STRING", "name:STRING"))
    private val errorSchema = ErrorSchemas.of(schema)

    /** 每次 bulk 提交进来的条数。 */
    private val submitted = mutableListOf<Int>()

    /** 下一次 bulk 返回的结果；为 null 时改抛 [bulkError]。 */
    private var bulkResponse: BulkResponse? = null
    private var bulkError: Throwable? = null

    private fun row(id: String, name: String? = "张三"): Row =
        Row.withSchema(schema).addValue(id).addValue(name).build()

    private fun window(): BoundedWindow = IntervalWindow(Instant(0), Duration.millis(10))

    private fun writeFn(batchSize: Int = 2, deadLetter: Boolean = true): EsWriteFn {
        val client = mock<ElasticsearchClient>()
        whenever(client.bulk(anyOrNull<BulkRequest>())).thenAnswer { inv ->
            submitted.add((inv.getArgument(0) as? BulkRequest)?.operations()?.size ?: 0)
            bulkError?.let { throw it } ?: bulkResponse
        }
        val fn = EsWriteFn(
            EsWriteConfig(
                connectionUri = "http://localhost:9200",
                index = "orders",
                schemaFields = listOf("id:STRING", "name:STRING"),
                batchSize = batchSize,
            ),
            schema,
            errorSchema,
            deadLetter,
            "WriteToElasticsearch8",
        )
        fn.clientFactory = EsClientFactory { client }
        fn.setup()
        return fn
    }

    private fun feed(fn: EsWriteFn, vararg rows: Row) =
        rows.forEach { fn.processElement(it, Instant(1), window(), PaneInfo.NO_FIRING) }

    /** 跑完一个 bundle，返回收集到的死信输出。 */
    private fun finishBundleOf(fn: EsWriteFn): CollectingFinishBundleContext<Row, Row> {
        val context = CollectingFinishBundleContext<Row, Row>()
        fn.finishBundle(context.context())
        return context
    }

    private fun elementOf(failureRow: Row): Row = failureRow.getValue(ErrorSchemas.ELEMENT)

    /** [failedAt] 是失败行在批次里的下标。 */
    private fun bulkOf(size: Int, failedAt: Set<Int> = emptySet()): BulkResponse {
        val items = (0 until size).map { i ->
            val item = mock<BulkResponseItem>()
            if (i in failedAt) {
                val cause = mock<ErrorCause>()
                whenever(cause.type()).doReturn("mapper_parsing_exception")
                whenever(cause.reason()).doReturn("failed to parse field [$i]")
                whenever(item.error()).doReturn(cause)
            } else {
                whenever(item.error()).doReturn(null)
            }
            item
        }
        val response = mock<BulkResponse>()
        whenever(response.errors()).doReturn(failedAt.isNotEmpty())
        whenever(response.items()).doReturn(items)
        return response
    }

    @Test
    fun `攒够 batch_size 才提交一次，而且一次提交整批`() {
        val fn = writeFn(batchSize = 2)
        bulkResponse = bulkOf(size = 2)

        feed(fn, row("a1"))
        assertEquals(0, submitted.size, "还没攒够一批，不该提前提交")

        feed(fn, row("a2"))

        assertEquals(listOf(2), submitted, "攒够 batch_size 应该只发一次 bulk，一次交整批")
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
    fun `bulk 的逐条结果里只有失败那行进死信`() {
        val fn = writeFn(batchSize = 3)
        bulkResponse = bulkOf(size = 3, failedAt = setOf(1))

        feed(fn, row("a1"), row("a2"), row("a3"))
        val context = finishBundleOf(fn)

        assertEquals(1, context.outputs.size)
        assertEquals("a2", elementOf(context.outputs[0]).getString("id"))
        val message = context.outputs[0].getString(ErrorSchemas.ERROR_MESSAGE)
        assertTrue(message!!.contains("failed to parse field [1]"), "死信要带真实的 ES 报错，实际: $message")
    }

    @Test
    fun `整批异常时全部行进死信，错误是真实异常`() {
        val fn = writeFn(batchSize = 2)
        bulkError = IllegalStateException("connection reset")

        feed(fn, row("a1"), row("a2"))
        val context = finishBundleOf(fn)

        assertEquals(listOf("a1", "a2"), context.outputs.map { elementOf(it).getString("id") })
        assertTrue(context.outputs.all { it.getString(ErrorSchemas.ERROR_MESSAGE)!!.contains("connection reset") })
    }

    @Test
    fun `死信带的是原始行自己的时间戳与窗口`() {
        val fn = writeFn(batchSize = 1)
        bulkError = IllegalStateException("cluster blocked")
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
        bulkError = IllegalStateException("boom")

        assertFailsWith<IllegalStateException> { feed(fn, row("a1")) }
    }

    @Test
    fun `没配 schema_fields 时按 document 列写，与读端的产出对得上`() {
        // 读端不声明 schema_fields 时产出的列就叫 document；写端曾经找的是 value，
        // 于是"读出来再写回去"这个文档里承诺的用法一行也走不通
        val client = mock<ElasticsearchClient>()
        val ok = bulkOf(size = 1)
        whenever(client.bulk(anyOrNull<BulkRequest>())).doReturn(ok)
        val documentSchema = buildSchema(emptyList())
        val fn = EsWriteFn(
            EsWriteConfig(connectionUri = "http://localhost:9200", index = "orders", batchSize = 1),
            documentSchema,
            ErrorSchemas.of(documentSchema),
            true,
            "WriteToElasticsearch8",
        )
        fn.clientFactory = EsClientFactory { client }
        fn.setup()

        val document = Row.withSchema(documentSchema).addValue("""{"id":"a1"}""").build()
        fn.processElement(document, Instant(1), window(), PaneInfo.NO_FIRING)
        val context = finishBundleOf(fn)

        assertEquals(0, context.outputs.size, "读端产出的 document 列必须能直接写回")
    }
}
