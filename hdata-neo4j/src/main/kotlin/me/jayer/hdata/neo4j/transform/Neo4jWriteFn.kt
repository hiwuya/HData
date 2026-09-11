package me.jayer.hdata.neo4j.transform

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.neo4j.Neo4jWriteConfig
import me.jayer.hdata.neo4j.internal.DriverFactory
import me.jayer.hdata.neo4j.internal.RealDriverFactory
import me.jayer.hdata.neo4j.internal.buildParams
import me.jayer.hdata.neo4j.internal.newSession
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.neo4j.driver.Driver
import org.slf4j.LoggerFactory

/**
 * Writes Neo4j by executing the Cypher `statement` with row fields bound as `$param` values.
 *
 * A batch of `batch_size` rows, or a finished bundle, commits in one transaction.
 *
 * If a batch commit fails, writes retry one row at a time to locate failed rows. `error_handling` sends those
 * records to dead letter; without it, the job fails.
 *
 * Each DoFn creates its connection in `@Setup` and closes it in `@Teardown`; transient fields preserve serializability.
 *
 * @author wuya
 */
class Neo4jWriteFn(
    private val config: Neo4jWriteConfig,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
    private val driverFactory: DriverFactory = RealDriverFactory,
) : DoFn<Row, Row>() {

    @Transient
    private var driver: Driver? = null

    @Transient
    private var buffered: MutableList<Pending>? = null

    @Transient
    private var failures: MutableList<ValueInSingleWindow<Row>>? = null

    private class Pending(val record: ValueInSingleWindow<Row>, val params: Map<String, Any?>)

    @Setup
    fun setup() {
        driver = driverFactory.create(config)
        buffered = mutableListOf()
        failures = mutableListOf()
    }

    @Teardown
    fun tearDown() {
        runCatching { driver?.close() }
        driver = null
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
        // A missing parameter is local to this row and must not prevent the rest of the batch from writing.
        val params = try {
            buildParams(row, config.statement, config.parameters)
        } catch (e: Exception) {
            reject(record, e)
            return
        }
        val queue = checkNotNull(buffered) { "writer is not initialized" }
        queue.add(Pending(record, params))
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
        try {
            writeBatch(queue)
            RECORDS_WRITTEN.inc(queue.size.toLong())
        } catch (e: Exception) {
            if (!deadLetter) {
                // The transaction rolled back. Without dead letter, keep all-or-nothing behavior instead of
                // partially writing rows that may be duplicated on a bundle retry.
                throw e
            }
            LOGGER.warn("Neo4j batch write failed; retrying one row at a time to identify failed data: {}", e.message)
            writeOneByOne(queue)
        } finally {
            queue.clear()
        }
    }

    /** Runs the complete batch in one transaction; failure rolls it back before [writeOneByOne] retries. */
    private fun writeBatch(queue: List<Pending>) {
        val d = driver ?: driverFactory.create(config).also { driver = it }
        newSession(d, config).use { session ->
            val tx = session.beginTransaction()
            try {
                queue.forEach { tx.run(config.statement, it.params).consume() }
                tx.commit()
            } catch (e: Exception) {
                runCatching { tx.rollback() }
                throw e
            } finally {
                runCatching { tx.close() }
            }
        }
    }

    private fun writeOneByOne(queue: List<Pending>) {
        val d = driver ?: driverFactory.create(config).also { driver = it }
        newSession(d, config).use { session ->
            queue.forEach { pending ->
                try {
                    session.run(config.statement, pending.params).consume()
                    RECORDS_WRITTEN.inc()
                } catch (e: Exception) {
                    reject(pending.record, e)
                }
            }
        }
    }

    private fun reject(record: ValueInSingleWindow<Row>, e: Exception) {
        if (!deadLetter) {
            throw e
        }
        LOGGER.warn("Neo4j write failed; sending record to dead letter: {}", e.message)
        RECORDS_REJECTED.inc()
        checkNotNull(failures).add(
            ValueInSingleWindow.of(
                ErrorSchemas.failure(errorSchema, record.value, e, transformName),
                record.timestamp,
                record.window,
                record.paneInfo,
            )
        )
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(Neo4jWriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(Neo4jWriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(Neo4jWriteFn::class.java, "records_rejected")
    }
}
