package me.jayer.hdata.clickhouse.transform

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.clickhouse.ClickHouseWriteConfig
import me.jayer.hdata.clickhouse.internal.ClickHouseJdbc
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.util.StringJoiner

/**
 * Writes rows to ClickHouse in batches via JDBC, with retries and dead-letter support.
 *
 * Each bundle accumulates up to [ClickHouseWriteConfig.batchSize] rows. When the batch is full or
 * the bundle ends, the rows are flushed via a JDBC batch INSERT. On transient failures, the batch
 * is retried up to [ClickHouseWriteConfig.maxRetries] times with exponential back-off.
 *
 * @author wuya
 */
class ClickHouseWriteFn(
    private val config: ClickHouseWriteConfig,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
) : DoFn<Row, Row>() {

    @Transient
    private var connection: Connection? = null

    @Transient
    private var pending: MutableList<ValueInSingleWindow<Row>>? = null

    @Transient
    private var failures: MutableList<ValueInSingleWindow<Row>>? = null

    @Transient
    private var resolvedColumns: List<String>? = null

    @Setup
    fun setup() {
        val jdbcUrl = ClickHouseJdbc.buildJdbcUrl(
            config.endpoint, config.database, config.connectTimeoutMs, config.socketTimeoutMs,
        )
        connection = DriverManager.getConnection(jdbcUrl, config.username, config.password)
        connection!!.autoCommit = false
        pending = mutableListOf()
        failures = mutableListOf()
    }

    @Teardown
    fun tearDown() {
        runCatching { flushPending() }
        runCatching { connection?.close() }
        connection = null
    }

    @StartBundle
    fun startBundle() {
        pending?.clear()
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

        // Resolve columns on first use.
        if (resolvedColumns == null) {
            resolvedColumns = config.resolvedColumns(records[0].value.schema.fieldNames.toList())
        }
        val columns = resolvedColumns!!

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
                    "ClickHouse batch write attempt {}/{} failed: {}",
                    attempt, maxAttempts, e.message,
                )
                if (attempt < maxAttempts) {
                    Thread.sleep(config.retryDelayMs * attempt)
                }
            }
        }

        if (!succeeded) {
            LOGGER.error("ClickHouse batch write failed after {} attempts", maxAttempts)
            records.forEach { reject(it, lastException!!) }
        }
    }

    private fun executeBatch(rows: List<Row>, columns: List<String>) {
        val conn = checkNotNull(connection) { "ClickHouse connection is not initialized" }

        val columnList = columns.joinToString(", ")
        val placeholders = columns.joinToString(", ") { "?" }
        val sql = "INSERT INTO ${config.table} ($columnList) VALUES ($placeholders)"

        val pstmt: PreparedStatement = conn.prepareStatement(sql)
        try {
            for (row in rows) {
                for (i in columns.indices) {
                    val colName = columns[i]
                    if (!row.schema.hasField(colName)) {
                        pstmt.setNull(i + 1, java.sql.Types.NULL)
                        continue
                    }
                    val value = row.getValue<Any?>(colName)
                    setParameter(pstmt, i + 1, value, row.schema.getField(colName).type)
                }
                pstmt.addBatch()
            }
            pstmt.executeBatch()
            conn.commit()
        } catch (e: Exception) {
            runCatching { conn.rollback() }
            throw e
        } finally {
            pstmt.close()
        }
    }

    private fun setParameter(pstmt: PreparedStatement, paramIndex: Int, value: Any?, fieldType: Schema.FieldType) {
        if (value == null) {
            pstmt.setNull(paramIndex, java.sql.Types.NULL)
            return
        }
        when (fieldType.typeName) {
            Schema.TypeName.STRING -> pstmt.setString(paramIndex, value.toString())
            Schema.TypeName.BYTE -> pstmt.setByte(paramIndex, (value as Number).toByte())
            Schema.TypeName.INT16 -> pstmt.setShort(paramIndex, (value as Number).toShort())
            Schema.TypeName.INT32 -> pstmt.setInt(paramIndex, (value as Number).toInt())
            Schema.TypeName.INT64 -> pstmt.setLong(paramIndex, (value as Number).toLong())
            Schema.TypeName.FLOAT -> pstmt.setFloat(paramIndex, (value as Number).toFloat())
            Schema.TypeName.DOUBLE -> pstmt.setDouble(paramIndex, (value as Number).toDouble())
            Schema.TypeName.BOOLEAN -> pstmt.setBoolean(paramIndex, when (value) {
                is Boolean -> value
                is Number -> value.toInt() != 0
                else -> value.toString().toBooleanStrictOrNull() ?: false
            })
            Schema.TypeName.DATETIME -> {
                val ldt = when (value) {
                    is LocalDateTime -> value
                    is LocalDate -> value.atStartOfDay()
                    is OffsetDateTime -> value.toLocalDateTime()
                    is java.sql.Timestamp -> value.toLocalDateTime()
                    is java.util.Date -> LocalDateTime.ofInstant(value.toInstant(), java.time.ZoneOffset.UTC)
                    else -> LocalDateTime.parse(value.toString())
                }
                pstmt.setTimestamp(paramIndex, java.sql.Timestamp.valueOf(ldt))
            }
            Schema.TypeName.DECIMAL -> {
                val bd = when (value) {
                    is java.math.BigDecimal -> value
                    is Number -> java.math.BigDecimal.valueOf(value.toDouble())
                    else -> java.math.BigDecimal(value.toString())
                }
                pstmt.setBigDecimal(paramIndex, bd)
            }
            Schema.TypeName.BYTES -> {
                val bytes = when (value) {
                    is ByteArray -> value
                    is String -> value.toByteArray(Charsets.UTF_8)
                    else -> value.toString().toByteArray(Charsets.UTF_8)
                }
                pstmt.setBytes(paramIndex, bytes)
            }
            else -> pstmt.setString(paramIndex, value.toString())
        }
    }

    private fun reject(record: ValueInSingleWindow<Row>, e: Exception) {
        if (!deadLetter) {
            throw e
        }
        LOGGER.warn("ClickHouse write failed; routing record to dead letter: {}", e.message)
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
        private val LOGGER = LoggerFactory.getLogger(ClickHouseWriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(ClickHouseWriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(ClickHouseWriteFn::class.java, "records_rejected")
    }
}
