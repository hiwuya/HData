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
 * Behavior tests for `Elasticsearch6WriteFn`'s **batching and dead letter**.
 *
 * `RestHighLevelClient`'s `bulk` is a final method, so this module uses the inline mock maker (see `mockito-inline` in
 * the pom); it still does not connect to a real ES.
 *
 * @author wuya
 */
class Elasticsearch6WriteBundleTest {

    private val fields = parseSchemaFields(listOf("id:STRING", "name:STRING"))
    private val schema = buildSchema(fields)
    private val errorSchema = ErrorSchemas.of(schema)

    /** The number of requests submitted on each `bulk` call. */
    private val submitted = mutableListOf<Int>()

    /** The result the next `bulk` returns; null means throw [bulkError] instead. */
    private var bulkResponse: BulkResponse? = null
    private var bulkError: Throwable? = null

    private fun row(id: String, name: String? = "John Doe"): Row =
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

    /** Runs one bundle to completion and returns the collected dead-letter outputs. */
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
        assertEquals(0, submitted.size, "a full batch has not accumulated yet, so it should not submit early")

        feed(fn, row("a2"))

        assertEquals(listOf(2), submitted, "once batch_size is reached it should send exactly one bulkRequest, submitting the whole batch at once")
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
        assertEquals(stamp, context.timestamps[0], "the dead letter must reuse the original row's timestamp; a fabricated Instant.now() is not replayable")
        assertEquals(win, context.windows[0], "the dead letter must reuse the original row's window; hardcoding GlobalWindow would throw in a windowed pipeline")
    }

    @Test
    fun `没开死信时写入失败直接抛异常`() {
        val fn = writeFn(batchSize = 1, deadLetter = false)
        bulkError = IOException("boom")

        assertFailsWith<IOException> { feed(fn, row("a1")) }
    }

    @Test
    fun `没配 schema_fields 时按 document 列写，与读端的产出对得上`() {
        // When the read side does not declare schema_fields, the column it produces is named document; the write side
        // used to look for value, so the documented usage of "read out then write back" did not work for a single row.
        val client = mock<RestHighLevelClient>()
        // bulkOf also uses whenever, so it must be fully evaluated before entering stubbing, otherwise UnfinishedStubbing is reported.
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

        assertEquals(0, context.outputs.size, "the document column produced by the read side must be writable back directly")
    }
}
