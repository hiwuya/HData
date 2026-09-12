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
 * Tests `EsWriteFn`'s **batching and dead-letter** behavior: a bulk request is only sent once
 * `batch_size` is reached, only the failed row from a bulk response's per-item results goes to the
 * dead letter, a whole-batch exception carries the real exception, and the dead letter keeps the
 * original row's timestamp and window.
 *
 * Uses a mocked [ElasticsearchClient]; no ES cluster required.
 *
 * @author wuya
 */
class EsWriteBundleTest {

    private val schema = buildSchema(listOf("id:STRING", "name:STRING"))
    private val errorSchema = ErrorSchemas.of(schema)

    /** Row count submitted on each bulk call. */
    private val submitted = mutableListOf<Int>()

    /** The result the next bulk call returns; throws [bulkError] instead when null. */
    private var bulkResponse: BulkResponse? = null
    private var bulkError: Throwable? = null

    private fun row(id: String, name: String? = "Alice"): Row =
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

    /** Runs one bundle to completion and returns the collected dead-letter output. */
    private fun finishBundleOf(fn: EsWriteFn): CollectingFinishBundleContext<Row, Row> {
        val context = CollectingFinishBundleContext<Row, Row>()
        fn.finishBundle(context.context())
        return context
    }

    private fun elementOf(failureRow: Row): Row = failureRow.getValue(ErrorSchemas.ELEMENT)

    /** [failedAt] is the index of the failed row within the batch. */
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
    fun `submits once batch_size is reached, and submits the whole batch in one call`() {
        val fn = writeFn(batchSize = 2)
        bulkResponse = bulkOf(size = 2)

        feed(fn, row("a1"))
        assertEquals(0, submitted.size, "a batch not yet full should not submit early")

        feed(fn, row("a2"))

        assertEquals(listOf(2), submitted, "reaching batch_size should send exactly one bulk call carrying the whole batch")
    }

    @Test
    fun `finishBundle flushes the trailing rows that don't fill a batch`() {
        val fn = writeFn(batchSize = 100)
        bulkResponse = bulkOf(size = 1)

        feed(fn, row("a1"))
        fn.finishBundle(CollectingFinishBundleContext<Row, Row>().context())

        assertEquals(listOf(1), submitted)
    }

    @Test
    fun `only the failed row from bulk's per-item results goes to the dead letter`() {
        val fn = writeFn(batchSize = 3)
        bulkResponse = bulkOf(size = 3, failedAt = setOf(1))

        feed(fn, row("a1"), row("a2"), row("a3"))
        val context = finishBundleOf(fn)

        assertEquals(1, context.outputs.size)
        assertEquals("a2", elementOf(context.outputs[0]).getString("id"))
        val message = context.outputs[0].getString(ErrorSchemas.ERROR_MESSAGE)
        assertTrue(message!!.contains("failed to parse field [1]"), "the dead letter should carry the real ES error, actual: $message")
    }

    @Test
    fun `a whole-batch exception sends every row to the dead letter, with the real exception`() {
        val fn = writeFn(batchSize = 2)
        bulkError = IllegalStateException("connection reset")

        feed(fn, row("a1"), row("a2"))
        val context = finishBundleOf(fn)

        assertEquals(listOf("a1", "a2"), context.outputs.map { elementOf(it).getString("id") })
        assertTrue(context.outputs.all { it.getString(ErrorSchemas.ERROR_MESSAGE)!!.contains("connection reset") })
    }

    @Test
    fun `the dead letter carries the original row's own timestamp and window`() {
        val fn = writeFn(batchSize = 1)
        bulkError = IllegalStateException("cluster blocked")
        val stamp = Instant(4242)
        val win = IntervalWindow(Instant(100), Duration.millis(100))

        fn.processElement(row("a1"), stamp, win, PaneInfo.NO_FIRING)
        val context = finishBundleOf(fn)

        assertEquals(1, context.outputs.size)
        assertEquals(stamp, context.timestamps[0], "the dead letter must reuse the original row's timestamp; fabricating Instant.now() cannot be replayed")
        assertEquals(win, context.windows[0], "the dead letter must reuse the original row's window; hardcoding GlobalWindow throws in a windowed pipeline")
    }

    @Test
    fun `with the dead letter off, a write failure throws directly`() {
        val fn = writeFn(batchSize = 1, deadLetter = false)
        bulkError = IllegalStateException("boom")

        assertFailsWith<IllegalStateException> { feed(fn, row("a1")) }
    }

    @Test
    fun `with no schema_fields configured, writes go through the document column, matching what the read side produces`() {
        // When the read side has no schema_fields declared, its output column is called document; the
        // write side used to look for value instead, so "read it out, then write it back" — a usage
        // this project's own docs promise — didn't actually work for a single row.
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

        assertEquals(0, context.outputs.size, "the document column the read side produces must be writable back directly")
    }
}
