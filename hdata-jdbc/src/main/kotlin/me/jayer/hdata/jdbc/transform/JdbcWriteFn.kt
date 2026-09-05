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
 * 攒批写入，死锁按退避策略重试。
 *
 * 输出的是**死信**记录：批量失败且开启了死信时会退回逐条写，只有真正写不进去的那几条才被吐出来。
 * 没开死信时异常直接抛出，作业失败。
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
        // 死信要带着原始记录的时间戳与窗口一起往下走，所以攒到 bundle 结束再统一输出
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
            LOGGER.warn("批量写入失败，退回逐条写入以定位坏数据: {}", e.message)
            writeOneByOne()
        } finally {
            // 未开死信时异常会继续抛出，也不能让旧批次残留在这个 DoFn 实例里。
            buffered.clear()
        }
    }

    private fun executeBatch(rows: List<Row>) {
        val sleeper = Sleeper.DEFAULT
        val backOff = checkNotNull(backoff) { "重试策略未初始化" }.backoff()
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
                    // 无论是否重试都要先回滚：重构前的不可重试分支直接抛出，把一个脏事务丢回连接池
                    rollbackQuietly(connection)
                    if (e !is SQLException || !isRetryable(e)) {
                        throw e
                    }
                    LOGGER.warn("检测到死锁，准备重试: {}", e.message)
                    if (!BackOffUtils.next(sleeper, backOff)) {
                        // 重试次数用尽
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
        checkNotNull(dataSource) { "数据源未初始化" }.connection

    private fun rollbackQuietly(connection: Connection) {
        runCatching { connection.rollback() }
            .onFailure { LOGGER.warn("回滚失败: {}", it.message) }
    }

    /** SQL state 40001 = 序列化失败，40P01 = PostgreSQL 死锁，都属于重试一次可能就过去的冲突。 */
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
