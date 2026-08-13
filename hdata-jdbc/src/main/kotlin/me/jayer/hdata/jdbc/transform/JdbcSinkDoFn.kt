package me.jayer.hdata.jdbc.transform

import com.zaxxer.hikari.HikariDataSource
import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.jdbc.JdbcWriteConfig
import me.jayer.hdata.jdbc.dataSourceProperties
import me.jayer.hdata.jdbc.statement.InsertStatement
import me.jayer.hdata.jdbc.strategy.DefaultRetryStrategy
import me.jayer.hdata.jdbc.strategy.RetryStrategy
import me.jayer.hdata.jdbc.type.JdbcTypeRegistry
import me.jayer.hdata.jdbc.util.JdbcUtils
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
import java.sql.SQLException
import java.util.concurrent.TimeUnit

/**
 * 攒批写入，死锁按退避策略重试。
 *
 * 输出的是**死信**记录：批量失败且开启了死信时会退回逐条写，只有真正写不进去的那几条才被吐出来。
 * 没开死信时异常直接抛出，行为与重构前一致。
 *
 * @author wuya
 * @date 2022-07-27
 */
class JdbcSinkDoFn(
    private val config: JdbcWriteConfig,
    private val inputSchema: Schema,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
) : DoFn<Row, Row>() {

    private lateinit var dataSource: HikariDataSource
    private lateinit var retryBackOff: FluentBackoff
    private lateinit var retryStrategy: RetryStrategy
    private lateinit var insertSql: String

    private val buffered = mutableListOf<ValueInSingleWindow<Row>>()
    private val failures = mutableListOf<ValueInSingleWindow<Row>>()

    @Setup
    fun setup() {
        dataSource = JdbcUtils.createDataSource(config.dataSourceProperties())
        retryBackOff = FluentBackoff.DEFAULT
            .withMaxRetries(config.retryMaxAttempts)
            .withInitialBackoff(Duration.standardSeconds(config.retryInitialSeconds))
            .withMaxBackoff(Duration.standardSeconds(config.retryMaxSeconds))
        retryStrategy = DefaultRetryStrategy()
        insertSql = InsertStatement(inputSchema.fieldNames, config.table).buildSql()
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
        // 死信要带着原始记录的时间戳与窗口一起往下走，所以攒到 bundle 结束再统一输出
        failures.forEach { context.output(it.value, it.timestamp, it.window) }
        failures.clear()
    }

    @Teardown
    fun tearDown() {
        if (::dataSource.isInitialized) {
            dataSource.close()
        }
    }

    private fun flush() {
        if (buffered.isEmpty()) {
            return
        }
        try {
            executeBatch(buffered.map { it.value })
        } catch (e: SQLException) {
            if (!deadLetter) {
                throw e
            }
            LOGGER.warn("批量写入失败，退回逐条写入以定位坏数据: {}", e.message)
            writeOneByOne()
        }
        buffered.clear()
    }

    private fun executeBatch(rows: List<Row>) {
        val sleeper = Sleeper.DEFAULT
        val backoff = retryBackOff.backoff()
        val startTime = System.nanoTime()
        dataSource.connection.use { connection ->
            connection.autoCommit = false
            while (true) {
                connection.prepareStatement(insertSql).use { ps ->
                    LOGGER.debug("Executing sql: {}", insertSql)
                    try {
                        rows.forEach { row ->
                            bindRow(ps, row)
                            ps.addBatch()
                        }
                        ps.executeBatch()
                        connection.commit()

                        RECORDS_PER_BATCH.update(rows.size.toLong())
                        MS_PER_BATCH.update(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime))
                        return
                    } catch (e: SQLException) {
                        if (!retryStrategy.apply(e)) {
                            throw e
                        }
                        LOGGER.warn("检测到死锁，准备重试", e)
                        ps.clearBatch()
                        connection.rollback()
                        if (!BackOffUtils.next(sleeper, backoff)) {
                            // 重试次数用尽
                            throw e
                        }
                    }
                }
            }
        }
    }

    private fun writeOneByOne() {
        dataSource.connection.use { connection ->
            connection.autoCommit = false
            buffered.forEach { record ->
                try {
                    writeSingle(connection, record.value)
                } catch (e: SQLException) {
                    FAILED_RECORDS.inc()
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

    private fun writeSingle(connection: Connection, row: Row) {
        connection.prepareStatement(insertSql).use { ps ->
            bindRow(ps, row)
            try {
                ps.executeUpdate()
                connection.commit()
            } catch (e: SQLException) {
                connection.rollback()
                throw e
            }
        }
    }

    private fun bindRow(ps: java.sql.PreparedStatement, row: Row) {
        for (index in 0 until inputSchema.fieldCount) {
            JdbcTypeRegistry.getPreparedStatementSetter(inputSchema.getField(index).type).setParameter(ps, row, index)
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(JdbcSinkDoFn::class.java)

        private val RECORDS_PER_BATCH = Metrics.distribution(JdbcSinkDoFn::class.java, "records_per_jdbc_batch")
        private val MS_PER_BATCH = Metrics.distribution(JdbcSinkDoFn::class.java, "milliseconds_per_batch")
        private val FAILED_RECORDS = Metrics.counter(JdbcSinkDoFn::class.java, "failed_records")
    }
}
