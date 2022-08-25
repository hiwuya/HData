package io.jayer.hdata.jdbc

import io.jayer.hdata.jdbc.handler.RowHandler
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.BoundedPerElement
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory
import java.sql.ResultSet


/**
 * @author wuya
 * @date 2022-07-27
 */
@BoundedPerElement
class JdbcSourceDoFn(private val rowHandler: RowHandler) : DoFn<JdbcSourceDescriptor, Row>() {

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(JdbcSourceDoFn::class.java)
    }

    @ProcessElement
    fun processElement(@Element sourceDescriptor: JdbcSourceDescriptor, receiver: OutputReceiver<Row>) {
        JdbcUtils.createDataSource(sourceDescriptor.dataSourceConfig).use { dataSource ->
            dataSource.connection.use { connection ->
                // PostgreSQL requires autocommit to be disabled to enable cursor streaming
                // see https://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor
                connection.autoCommit = false
                val sql = sourceDescriptor.createQuery()
                connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { ps ->
                    ps.fetchSize = sourceDescriptor.fetchSize
                    LOGGER.info("Executing query: {}", sql)
                    ps.executeQuery().use { rs ->
                        while (rs.next()) {
                            receiver.output(rowHandler.handle(rs))
                        }
                    }
                }
            }
        }
    }
}