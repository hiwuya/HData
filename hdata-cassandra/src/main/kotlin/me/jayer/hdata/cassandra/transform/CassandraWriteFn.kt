package me.jayer.hdata.cassandra.transform

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel
import com.datastax.oss.driver.api.core.cql.BatchStatement
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder
import com.datastax.oss.driver.api.core.cql.BatchType
import com.datastax.oss.driver.api.core.cql.BoundStatement
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.cassandra.CassandraWriteConfig
import me.jayer.hdata.cassandra.internal.CassandraSessions
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset

/**
 * Writes rows to Cassandra in batches via CQL INSERT, with retries and dead-letter support.
 *
 * Each bundle accumulates up to [CassandraWriteConfig.batchSize] rows. When the batch is full or
 * the bundle ends, the rows are flushed via a CQL batch statement. On transient failures, the
 * batch is retried up to [CassandraWriteConfig.maxRetries] times with exponential back-off.
 *
 * @author wuya
 */
class CassandraWriteFn(
    private val config: CassandraWriteConfig,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
) : DoFn<Row, Row>() {

    @Transient
    private var session: CqlSession? = null

    @Transient
    private var pending: MutableList<ValueInSingleWindow<Row>>? = null

    @Transient
    private var failures: MutableList<ValueInSingleWindow<Row>>? = null

    @Transient
    private var preparedStatement: PreparedStatement? = null

    @Transient
    private var lastColumns: List<String>? = null

    @Setup
    fun setup() {
        session = CassandraSessions.newSession(
            config.endpoints, config.keyspace, config.connectTimeoutMs, config.requestTimeoutMs,
        )
        pending = mutableListOf()
        failures = mutableListOf()
    }

    @Teardown
    fun tearDown() {
        runCatching { flushPending() }
        runCatching { session?.close() }
        session = null
    }

    @StartBundle
    fun startBundle() {
        pending?.clear()
        failures?.clear()
        preparedStatement = null
        lastColumns = null
    }

    @ProcessElement
    fun processElement(
        @Element row: Row,
        @Timestamp timestamp: org.joda.time.Instant,
        window: BoundedWindow,
        pane: PaneInfo,
    ) {
        val record = ValueInSingleWindow.of(row, timestamp, window, pane)
        pending!!.add(record)

        if (pending!!.size >= config.batchSize) {
            flushPending()
        }
    }

    @FinishBundle
    fun finishBundle(context: FinishBundleContext) {
        flushPending()
        val rejected = checkNotNull(failures)
        rejected.forEach { context.output(it.value, it.timestamp, it.window) }
        rejected.clear()
    }

    private fun flushPending() {
        val queue = checkNotNull(pending)
        if (queue.isEmpty()) return

        val records = queue.toList()
        queue.clear()

        if (records.isEmpty()) return

        // Resolve columns from first row's schema.
        val columns = records[0].value.schema.fieldNames.toList()
        if (lastColumns != columns) {
            preparedStatement = null // Schema changed, re-prepare.
            lastColumns = columns
        }

        var lastException: Exception? = null
        var succeeded = false
        val maxAttempts = config.maxRetries + 1

        for (attempt in 1..maxAttempts) {
            try {
                executeBatch(records.map { it.value }, columns)
                RECORDS_WRITTEN.inc(records.size.toLong())
                succeeded = true
                break
            } catch (e: Exception) {
                lastException = e
                LOGGER.warn(
                    "Cassandra batch write attempt {}/{} failed: {}",
                    attempt, maxAttempts, e.message,
                )
                if (attempt < maxAttempts) {
                    Thread.sleep(config.retryDelayMs * attempt)
                }
            }
        }

        if (!succeeded) {
            LOGGER.error("Cassandra batch write failed after {} attempts", maxAttempts)
            records.forEach { reject(it, lastException!!) }
        }
    }

    private fun executeBatch(rows: List<Row>, columns: List<String>) {
        val s = checkNotNull(session) { "Cassandra session is not initialized" }
        val consistencyLevel = DefaultConsistencyLevel.valueOf(config.consistencyLevel)

        // Prepare INSERT statement once.
        val ps = preparedStatement ?: run {
            val columnList = columns.joinToString(", ")
            val placeholders = columns.joinToString(", ") { "?" }
            val sql = "INSERT INTO ${config.table} ($columnList) VALUES ($placeholders)"
            val prepared = s.prepare(sql)
            preparedStatement = prepared
            prepared
        }

        val batchType = if (config.unloggedBatch) BatchType.UNLOGGED else BatchType.LOGGED
        val batchBuilder = BatchStatement.builder(batchType)
            .setConsistencyLevel(consistencyLevel)

        for (row in rows) {
            val bound = ps.bind()
            for (i in columns.indices) {
                val colName = columns[i]
                if (!row.schema.hasField(colName)) {
                    bound.setToNull(i)
                    continue
                }
                val value = row.getValue<Any?>(colName)
                setParameter(bound, i, value, row.schema.getField(colName).type)
            }
            batchBuilder.addStatement(bound)
        }

        val batch: BatchStatement = batchBuilder.build()
        s.execute(batch)
    }

    private fun setParameter(bound: BoundStatement, index: Int, value: Any?, fieldType: Schema.FieldType) {
        if (value == null) {
            bound.setToNull(index)
            return
        }
        when (fieldType.typeName) {
            Schema.TypeName.STRING -> bound.setString(index, value.toString())
            Schema.TypeName.BYTE -> bound.setByte(index, (value as Number).toByte())
            Schema.TypeName.INT16 -> bound.setShort(index, (value as Number).toShort())
            Schema.TypeName.INT32 -> bound.setInt(index, (value as Number).toInt())
            Schema.TypeName.INT64 -> bound.setLong(index, (value as Number).toLong())
            Schema.TypeName.FLOAT -> bound.setFloat(index, (value as Number).toFloat())
            Schema.TypeName.DOUBLE -> bound.setDouble(index, (value as Number).toDouble())
            Schema.TypeName.BOOLEAN -> bound.setBoolean(index, when (value) {
                is Boolean -> value
                is Number -> value.toInt() != 0
                else -> value.toString().toBooleanStrictOrNull() ?: false
            })
            Schema.TypeName.DECIMAL -> {
                val bd = when (value) {
                    is java.math.BigDecimal -> value
                    is Number -> java.math.BigDecimal.valueOf(value.toDouble())
                    else -> java.math.BigDecimal(value.toString())
                }
                bound.setBigDecimal(index, bd)
            }
            Schema.TypeName.DATETIME -> {
                val instant = when (value) {
                    is Instant -> value
                    is LocalDateTime -> value.toInstant(ZoneOffset.UTC)
                    is LocalDate -> value.atStartOfDay().toInstant(ZoneOffset.UTC)
                    is OffsetDateTime -> value.toInstant()
                    is java.sql.Timestamp -> value.toInstant()
                    is java.util.Date -> value.toInstant()
                    else -> Instant.parse(value.toString())
                }
                bound.setInstant(index, instant)
            }
            Schema.TypeName.BYTES -> {
                val bytes = when (value) {
                    is java.nio.ByteBuffer -> value
                    is ByteArray -> java.nio.ByteBuffer.wrap(value)
                    is String -> java.nio.ByteBuffer.wrap(value.toByteArray(Charsets.UTF_8))
                    else -> java.nio.ByteBuffer.wrap(value.toString().toByteArray(Charsets.UTF_8))
                }
                bound.setByteBuffer(index, bytes)
            }
            else -> bound.setString(index, value.toString())
        }
    }

    private fun reject(record: ValueInSingleWindow<Row>, e: Exception) {
        if (!deadLetter) {
            throw e
        }
        LOGGER.warn("Cassandra write failed; routing record to dead letter: {}", e.message)
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
        private val LOGGER = LoggerFactory.getLogger(CassandraWriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(CassandraWriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(CassandraWriteFn::class.java, "records_rejected")
    }
}
