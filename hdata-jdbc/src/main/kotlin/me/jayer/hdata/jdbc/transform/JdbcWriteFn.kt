package me.jayer.hdata.jdbc.transform

import com.zaxxer.hikari.HikariDataSource
import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.jdbc.JdbcWriteConfig
import me.jayer.hdata.jdbc.dataSourceProperties
import me.jayer.hdata.jdbc.internal.DataSources
import me.jayer.hdata.jdbc.internal.InsertSql
import me.jayer.hdata.jdbc.internal.RowBinder
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.util.BackOffUtils
import org.apache.beam.sdk.util.FluentBackoff
import org.apache.beam.sdk.util.Sleeper
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.joda.time.Duration
import org.joda.time.Instant
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import java.util.concurrent.TimeUnit

/**
 * Buffers rows into batches for writing, retrying deadlocks with a backoff policy.
 *
 * The output consists of **dead-letter** records: when a batch fails and dead letter is enabled, we fall back to per-row writes,
 * and only the rows that really cannot be written are emitted. Without dead letter the exception is rethrown and the job fails.
 *
 * @author wuya
 * @date 2022-07-27
 */
class JdbcWriteFn(
    private val config: JdbcWriteConfig,
    private val inputSchema: Schema,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
) : DoFn<Row, Row>() {

    @Transient
    private var dataSource: HikariDataSource? = null

    @Transient
    private var backoff: FluentBackoff? = null

    private val insertSql: String = InsertSql.render(config.table, inputSchema.fieldNames)
    private val binder: RowBinder = RowBinder.of(inputSchema)

    private val buffered = mutableListOf<ValueInSingleWindow<Row>>()
    private val failures = mutableListOf<ValueInSingleWindow<Row>>()

    @Setup
    fun setup() {
        dataSource = DataSources.create(config.dataSourceProperties(), "hdata-jdbc-write")
        backoff = FluentBackoff.DEFAULT
            .withMaxRetries(config.retryMaxAttempts)
            .withInitialBackoff(Duration.standardSeconds(config.retryInitialSeconds))
            .withMaxBackoff(Duration.standardSeconds(config.retryMaxSeconds))
    }

    @StartBundle
    fun startBundle() {
        buffered.clear()
        failures.clear()
    }

    @ProcessElement
    fun processElement(
        @Element row: Row,
        @Timestamp timestamp: Instant,
        window: BoundedWindow,
        pane: PaneInfo,
    ) {
        buffered.add(ValueInSingleWindow.of(row, timestamp, window, pane))
        if (buffered.size >= config.batchSize) {
            flush()
        }
    }

    @FinishBundle
    fun finishBundle(context: FinishBundleContext) {
        flush()
        // A dead letter must carry the timestamp and window of the original record downstream, so buffer until the bundle ends and emit everything at once
        failures.forEach { context.output(it.value, it.timestamp, it.window) }
        failures.clear()
    }

    @Teardown
    fun tearDown() {
        dataSource?.close()
        dataSource = null
    }

    private fun flush() {
        if (buffered.isEmpty()) {
            return
        }
        try {
            executeBatch(buffered.map { it.value })
        } catch (e: Exception) {
            if (!deadLetter) {
                throw e
            }
            LOGGER.warn("bulk write failed, falling back to per-row writes to locate the bad data: {}", e.message)
            writeOneByOne()
        } finally {
            // Without dead letter the exception keeps propagating, and no stale batch may be left behind in this DoFn instance either.
            buffered.clear()
        }
    }

    private fun executeBatch(rows: List<Row>) {
        val sleeper = Sleeper.DEFAULT
        val backOff = checkNotNull(backoff) { "retry policy is not initialized" }.backoff()
        val startTime = System.nanoTime()
        connection().use { connection ->
            connection.autoCommit = false
            while (true) {
                try {
                    connection.prepareStatement(insertSql).use { ps ->
                        rows.forEach { row ->
                            binder.bind(ps, row)
                            ps.addBatch()
                        }
                        ps.executeBatch()
                    }
                    connection.commit()
                    RECORDS_WRITTEN.inc(rows.size.toLong())
                    RECORDS_PER_BATCH.update(rows.size.toLong())
                    MS_PER_BATCH.update(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime))
                    return
                } catch (e: Exception) {
                    // Always roll back first, whether we retry or not: the non-retryable branch used to throw right away, handing a dirty transaction back to the pool
                    rollbackQuietly(connection)
                    if (e !is SQLException || !isRetryable(e)) {
                        throw e
                    }
                    LOGGER.warn("deadlock detected, preparing to retry: {}", e.message)
                    if (!BackOffUtils.next(sleeper, backOff)) {
                        // Retry attempts exhausted
                        throw e
                    }
                }
            }
        }
    }

    private fun writeOneByOne() {
        connection().use { connection ->
            connection.autoCommit = false
            connection.prepareStatement(insertSql).use { ps ->
                buffered.forEach { record ->
                    try {
                        writeSingle(connection, ps, record.value)
                        RECORDS_WRITTEN.inc()
                    } catch (e: Exception) {
                        RECORDS_REJECTED.inc()
                        failures.add(
                            ValueInSingleWindow.of(
                                ErrorSchemas.failure(errorSchema, record.value, e, transformName),
                                record.timestamp,
                                record.window,
                                record.paneInfo,
                            )
                        )
                    }
                }
            }
        }
    }

    private fun writeSingle(connection: Connection, ps: PreparedStatement, row: Row) {
        try {
            binder.bind(ps, row)
            ps.executeUpdate()
            connection.commit()
        } catch (e: Exception) {
            rollbackQuietly(connection)
            throw e
        } finally {
            runCatching { ps.clearParameters() }
        }
    }

    private fun connection(): Connection =
        checkNotNull(dataSource) { "data source is not initialized" }.connection

    private fun rollbackQuietly(connection: Connection) {
        runCatching { connection.rollback() }
            .onFailure { LOGGER.warn("rollback failed: {}", it.message) }
    }

    /** SQL state 40001 = serialization failure, 40P01 = PostgreSQL deadlock; both are conflicts a retry may well get past. */
    private fun isRetryable(e: SQLException): Boolean = e.sqlState in RETRYABLE_SQL_STATES

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(JdbcWriteFn::class.java)

        private val RETRYABLE_SQL_STATES = setOf("40001", "40P01")

        private val RECORDS_WRITTEN = Metrics.counter(JdbcWriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(JdbcWriteFn::class.java, "records_rejected")
        private val RECORDS_PER_BATCH = Metrics.distribution(JdbcWriteFn::class.java, "records_per_jdbc_batch")
        private val MS_PER_BATCH = Metrics.distribution(JdbcWriteFn::class.java, "milliseconds_per_batch")
    }
}
