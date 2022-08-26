package io.jayer.hdata.jdbc.transform

import com.zaxxer.hikari.HikariDataSource
import io.jayer.hdata.jdbc.JdbcSinkDescriptor
import io.jayer.hdata.jdbc.JdbcUtils
import io.jayer.hdata.jdbc.statement.InsertStatement
import io.jayer.hdata.jdbc.type.JdbcTypeRegistry
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory


/**
 * @author wuya
 * @date 2022-07-27
 */
class JdbcSinkDoFn(private val sinkDescriptor: JdbcSinkDescriptor) : DoFn<Row, Void>() {

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(JdbcSinkDoFn::class.java)
    }

    private lateinit var dataSource: HikariDataSource
    private val rows = mutableListOf<Row>()

    @Setup
    fun setup() {
        dataSource = JdbcUtils.createDataSource(sinkDescriptor.dataSourceConfig)
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
        val columns = rows.first().schema.fieldNames
        val sql = InsertStatement(columns, sinkDescriptor.table).buildSql()
        dataSource.connection.use { connection ->
            connection.autoCommit = false
            connection.prepareStatement(sql).use { ps ->
                LOGGER.info("Executing sql: {}", sql)
                try {
                    rows.forEach { row ->
                        val schema = row.schema
                        for (i in 0 until schema.fieldCount) {
                            val fieldType = schema.getField(i).type
                            JdbcTypeRegistry.getPreparedStatementSetter(fieldType).setParameter(ps, row, i)
                        }

                        ps.addBatch()
                    }
                    ps.executeBatch()
                    connection.commit()
                } catch (e: Exception) {
                    LOGGER.error("SQL exception thrown while writing to JDBC database: {}", e.message)
                    connection.rollback()
                    throw e
                }
            }
        }
    }

    @Teardown
    fun tearDown() {
        dataSource.close()
    }
}