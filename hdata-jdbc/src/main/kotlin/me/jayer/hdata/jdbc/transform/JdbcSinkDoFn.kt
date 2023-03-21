package me.jayer.hdata.jdbc.transform

import com.zaxxer.hikari.HikariDataSource
import me.jayer.hdata.jdbc.JdbcSinkDescriptor
import me.jayer.hdata.jdbc.util.JdbcUtils
import me.jayer.hdata.jdbc.statement.InsertStatement
import me.jayer.hdata.jdbc.strategy.DefaultRetryStrategy
import me.jayer.hdata.jdbc.strategy.RetryStrategy
import me.jayer.hdata.jdbc.type.JdbcTypeRegistry
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.util.BackOffUtils
import org.apache.beam.sdk.util.FluentBackoff
import org.apache.beam.sdk.util.Sleeper
import org.apache.beam.sdk.values.Row
import org.joda.time.Duration
import org.slf4j.LoggerFactory
import java.sql.SQLException
import java.util.concurrent.TimeUnit


/**
 * @author wuya
 * @date 2022-07-27
 */
class JdbcSinkDoFn(private val sinkDescriptor: me.jayer.hdata.jdbc.JdbcSinkDescriptor) : DoFn<Row, Void>() {

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(me.jayer.hdata.jdbc.transform.JdbcSinkDoFn::class.java)

        private val RECORDS_PER_BATCH = Metrics.distribution(me.jayer.hdata.jdbc.transform.JdbcSinkDoFn::class.java, "records_per_jdbc_batch")
        private val MS_PER_BATCH = Metrics.distribution(me.jayer.hdata.jdbc.transform.JdbcSinkDoFn::class.java, "milliseconds_per_batch")
    }

    private lateinit var dataSource: HikariDataSource
    private lateinit var retryBackOff: FluentBackoff
    private lateinit var retryStrategy: me.jayer.hdata.jdbc.strategy.RetryStrategy
    private val rows = mutableListOf<Row>()

    @Setup
    fun setup() {
        dataSource = me.jayer.hdata.jdbc.util.JdbcUtils.createDataSource(sinkDescriptor.dataSourceConfig)
        retryBackOff = FluentBackoff.DEFAULT.withMaxRetries(sinkDescriptor.retryMaxAttempts)
            .withInitialBackoff(Duration.standardSeconds(sinkDescriptor.retryInitialSeconds))
            .withMaxBackoff(Duration.standardSeconds(sinkDescriptor.retryMaxSeconds))
        retryStrategy = me.jayer.hdata.jdbc.strategy.DefaultRetryStrategy()
    }

    @ProcessElement
    fun processElement(context: ProcessContext) {
        rows.add(context.element())
        if (rows.size >= sinkDescriptor.batchSize) {
            executeBatch(rows)
            rows.clear()
        }
    }

    @FinishBundle
    fun finishBundle() {
        if (rows.isNotEmpty()) {
            executeBatch(rows)
        }
    }

    private fun executeBatch(rows: List<Row>) {
        val sleeper = Sleeper.DEFAULT
        val backoff = retryBackOff.backoff()
        val columns = rows.first().schema.fieldNames
        val sql = me.jayer.hdata.jdbc.statement.InsertStatement(columns, sinkDescriptor.table).buildSql()
        dataSource.connection.use { connection ->
            connection.autoCommit = false
            val startTime = System.nanoTime()
            while (true) {
                connection.prepareStatement(sql).use { ps ->
                    me.jayer.hdata.jdbc.transform.JdbcSinkDoFn.Companion.LOGGER.info("Executing sql: {}", sql)
                    try {
                        rows.forEach { row ->
                            val schema = row.schema
                            for (i in 0 until schema.fieldCount) {
                                val fieldType = schema.getField(i).type
                                me.jayer.hdata.jdbc.type.JdbcTypeRegistry.getPreparedStatementSetter(fieldType).setParameter(ps, row, i)
                            }

                            ps.addBatch()
                        }
                        ps.executeBatch()
                        connection.commit()

                        me.jayer.hdata.jdbc.transform.JdbcSinkDoFn.Companion.RECORDS_PER_BATCH.update(rows.size.toLong())
                        me.jayer.hdata.jdbc.transform.JdbcSinkDoFn.Companion.MS_PER_BATCH.update(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime))
                        return
                    } catch (e: SQLException) {
                        me.jayer.hdata.jdbc.transform.JdbcSinkDoFn.Companion.LOGGER.trace("SQL exception thrown while writing to JDBC database: {}", e.message)
                        if (!retryStrategy.apply(e)) {
                            throw e
                        }

                        me.jayer.hdata.jdbc.transform.JdbcSinkDoFn.Companion.LOGGER.warn("Deadlock detected, retrying...", e)
                        ps.clearBatch()
                        connection.rollback()

                        if (!BackOffUtils.next(sleeper, backoff)) {
                            // we tried the max number of times
                            throw e
                        }
                    }
                }
            }
        }
    }

    @Teardown
    fun tearDown() {
        dataSource.close()
    }
}