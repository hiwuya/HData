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
 * **Batching and dead-letter** behavior tests for `HBaseWriteFn`.
 *
 * The focus is `Table.batch`'s **per-row result**: `results[i]` being a `Throwable` means that row failed,
 * `null` means the row was never attempted, anything else means it succeeded. The pre-refactor implementation
 * only checked "did the whole batch throw", so one bad row would mark the entire batch as failed, and the
 * dead-letter record's `element` was null with a freshly-made-up timestamp.
 *
 * Uses a mocked [Table]; no HBase cluster required.
 *
 * @author wuya
 */
class HBaseWriteBundleTest {

    private val codec = HBaseRowCodec.of("rowkey", "string", listOf("name:STRING", "age:INT32"), "cf")
    private val errorSchema = ErrorSchemas.of(codec.schema)

    /** Batches submitted via `batch` so far. */
    private val submitted = mutableListOf<List<Put>>()

    /** What the next `batch` call fills `results` with: a Throwable for a failed row, null for not attempted, anything else for success. */
    private var batchResults: List<Any?> = emptyList()

    /** When non-null, the next `batch` call throws it after filling in results. */
    private var batchError: Throwable? = null

    private fun row(rowkey: String, name: String? = "Alice", age: Int? = 30): Row =
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

    /** Runs one bundle to completion and returns the collected dead-letter output. */
    private fun finishBundleOf(fn: HBaseWriteFn): CollectingFinishBundleContext<Row, Row> {
        val context = CollectingFinishBundleContext<Row, Row>()
        fn.finishBundle(context.context())
        return context
    }

    private fun elementOf(failureRow: Row): Row = failureRow.getValue(ErrorSchemas.ELEMENT)

    @Test
    fun `only submits once batch_size is reached, and submits the whole batch at once`() {
        val fn = writeFn(batchSize = 2)

        feed(fn, row("r1"))
        assertEquals(0, submitted.size, "not enough rows buffered yet, should not submit early")

        feed(fn, row("r2"))

        assertEquals(1, submitted.size, "reaching batch_size should trigger exactly one batch call")
        assertEquals(2, submitted[0].size, "the whole batch should be handed over at once, not row by row")
    }

    @Test
    fun `finishBundle flushes the trailing partial batch`() {
        val fn = writeFn(batchSize = 100)

        feed(fn, row("r1"))
        fn.finishBundle(CollectingFinishBundleContext<Row, Row>().context())

        assertEquals(1, submitted.size)
        assertEquals(1, submitted[0].size)
    }

    @Test
    fun `only the row whose per-row result is a Throwable goes to the dead letter`() {
        val fn = writeFn(batchSize = 3)
        batchResults = listOf(Any(), IOException("NotServingRegion"), Any())

        feed(fn, row("r1"), row("r2"), row("r3"))
        val context = finishBundleOf(fn)

        assertEquals(1, context.outputs.size, "only the row whose result is a Throwable should fail")
        assertEquals("r2", elementOf(context.outputs[0]).getString("rowkey"))
        val message = context.outputs[0].getString(ErrorSchemas.ERROR_MESSAGE)
        assertTrue(message!!.contains("NotServingRegion"), "the dead letter should carry the real HBase exception, got: $message")
    }

    @Test
    fun `when the whole batch call throws, every row goes to the dead letter with the real exception`() {
        val fn = writeFn(batchSize = 2)
        batchError = IOException("connection reset")

        feed(fn, row("r1"), row("r2"))
        val context = finishBundleOf(fn)

        // results are all null, meaning no row got a turn; use the batch-level exception here instead of making one up
        assertEquals(listOf("r1", "r2"), context.outputs.map { elementOf(it).getString("rowkey") })
        assertTrue(context.outputs.all { it.getString(ErrorSchemas.ERROR_MESSAGE)!!.contains("connection reset") })
    }

    @Test
    fun `the dead letter carries the original row's own timestamp and window`() {
        val fn = writeFn(batchSize = 1)
        batchError = IOException("region offline")
        val stamp = Instant(4242)
        val win = IntervalWindow(Instant(100), Duration.millis(100))

        fn.processElement(row("r1"), stamp, win, PaneInfo.NO_FIRING)
        val context = finishBundleOf(fn)

        assertEquals(1, context.outputs.size)
        assertEquals(stamp, context.timestamps[0], "the dead letter must reuse the original row's timestamp; a freshly-made Instant.now() cannot be replayed")
        assertEquals(win, context.windows[0], "the dead letter must reuse the original row's window; hardcoding GlobalWindow would throw in a windowed pipeline")
    }

    @Test
    fun `the queue is cleared after success, so rerunning the same bundle does not resubmit`() {
        val fn = writeFn(batchSize = 10)

        feed(fn, row("r1"), row("r2"))
        fn.finishBundle(CollectingFinishBundleContext<Row, Row>().context())
        fn.startBundle()
        fn.finishBundle(CollectingFinishBundleContext<Row, Row>().context())

        assertEquals(1, submitted.size, "the batch must be cleared after submission, otherwise a runner retry of the same bundle would rewrite the old rows")
    }

    @Test
    fun `without the dead letter enabled, a write failure throws directly`() {
        val fn = writeFn(batchSize = 1, deadLetter = false)
        batchError = IOException("boom")

        assertFailsWith<IOException> { feed(fn, row("r1")) }
    }

    @Test
    fun `a row whose columns are all null is rejected while building the Put and never reaches the batch`() {
        val fn = writeFn(batchSize = 1)

        feed(fn, row("r1", name = null, age = null))
        val context = finishBundleOf(fn)

        assertEquals(0, submitted.size, "an empty Put should not be sent to HBase")
        assertEquals(1, context.outputs.size)
        assertTrue(context.outputs[0].getString(ErrorSchemas.ERROR_MESSAGE)!!.contains("r1"))
    }
}
