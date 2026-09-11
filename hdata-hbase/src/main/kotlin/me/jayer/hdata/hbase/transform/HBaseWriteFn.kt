package me.jayer.hdata.hbase.transform

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.hbase.HBaseConnections
import me.jayer.hdata.hbase.HBaseRowCodec
import me.jayer.hdata.hbase.HBaseWriteConfig
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Table
import org.slf4j.LoggerFactory

/**
 * Buffers HBase writes and optionally emits dead-letter records.
 *
 * Submission uses `Table.batch(actions, results)` instead of `Table.put(list)`, allowing each result to be mapped
 * back to its row. Successful entries return `Result`, failures return `Throwable`, and null means the row was not attempted.
 *
 * @author wuya
 */
class HBaseWriteFn(
    private val config: HBaseWriteConfig,
    private val codec: HBaseRowCodec,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
) : DoFn<Row, Row>() {

    @Transient
    private var connection: Connection? = null

    @Transient
    private var table: Table? = null

    @Transient
    private var buffered: MutableList<Pending>? = null

    @Transient
    private var failures: MutableList<ValueInSingleWindow<Row>>? = null

    /**
     * Test-only table injection; production uses null. It is intentionally not transient so DirectRunner
     * serializes the fake table into workers during tests.
     */
    internal var testTable: Table? = null

    private class Pending(val record: ValueInSingleWindow<Row>, val put: Put)

    @Setup
    fun setup() {
        buffered = mutableListOf()
        failures = mutableListOf()
        val injected = testTable
        if (injected != null) {
            table = injected
            return
        }
        val conn = HBaseConnections.newConnection(config.configuration())
        connection = conn
        // Table handles are lightweight, but recreating one for each flush is unnecessary; keep it with the connection.
        table = conn.getTable(TableName.valueOf(config.table))
    }

    @Teardown
    fun tearDown() {
        runCatching { table?.close() }
        runCatching { connection?.close() }
        table = null
        connection = null
    }

    @StartBundle
    fun startBundle() {
        buffered?.clear()
        failures?.clear()
    }

    @ProcessElement
    fun processElement(
        @Element row: Row,
        @Timestamp timestamp: org.joda.time.Instant,
        window: BoundedWindow,
        pane: PaneInfo,
    ) {
        val record = ValueInSingleWindow.of(row, timestamp, window, pane)
        val put = try {
            codec.toPut(row)
        } catch (e: Exception) {
            reject(record, e)
            return
        }
        val queue = checkNotNull(buffered) { "writer is not initialized" }
        queue.add(Pending(record, put))
        if (queue.size >= config.batchSize) {
            flush()
        }
    }

    @FinishBundle
    fun finishBundle(context: FinishBundleContext) {
        flush()
        val rejected = checkNotNull(failures)
        rejected.forEach { context.output(it.value, it.timestamp, it.window) }
        rejected.clear()
    }

    private fun flush() {
        val queue = checkNotNull(buffered)
        if (queue.isEmpty()) {
            return
        }
        val results = arrayOfNulls<Any>(queue.size)
        var batchError: Exception? = null
        try {
            checkNotNull(table).batch(queue.map { it.put }, results)
        } catch (e: Exception) {
            // A partial batch can throw after filling results; per-row results are more precise than the exception.
            batchError = e
        }
        try {
            queue.forEachIndexed { index, pending ->
                when (val result = results[index]) {
                    is Throwable -> reject(pending.record, result.asException())
                    // Null means this row was never attempted because the batch failed before submission.
                    null -> reject(pending.record, batchError ?: IllegalStateException("HBase returned no result for this row"))
                    else -> RECORDS_WRITTEN.inc()
                }
            }
        } finally {
            // Clear the batch even when reject throws, preventing stale rows from leaking into retries.
            queue.clear()
        }
    }

    private fun Throwable.asException(): Exception = this as? Exception ?: RuntimeException(this)

    private fun reject(record: ValueInSingleWindow<Row>, e: Exception) {
        if (!deadLetter) {
            throw e
        }
        LOGGER.warn("HBase write failed; sending record to dead letter: {}", e.message)
        RECORDS_REJECTED.inc()
        checkNotNull(failures).add(
            ValueInSingleWindow.of(
                // Preserve the original row timestamp and window so the record can be replayed.
                ErrorSchemas.failure(errorSchema, record.value, e, transformName),
                record.timestamp,
                record.window,
                record.paneInfo,
            )
        )
    }


    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(HBaseWriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(HBaseWriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(HBaseWriteFn::class.java, "records_rejected")
    }
}
