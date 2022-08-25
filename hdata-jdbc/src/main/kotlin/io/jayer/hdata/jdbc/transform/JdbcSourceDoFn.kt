package io.jayer.hdata.jdbc.transform

import io.jayer.hdata.jdbc.JdbcUtils
import io.jayer.hdata.jdbc.handler.RowHandler
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.util.*


/**
 * @author wuya
 * @date 2022-07-27
 */
class JdbcSourceDoFn(
    private val dataSourceConfig: Properties,
    private val query: String,
    private val fetchSize: Int,
    private val rowHandler: RowHandler,
) : DoFn<Void, Row>() {

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(JdbcSourceDoFn::class.java)
    }

    @ProcessElement
    fun processElement(receiver: OutputReceiver<Row>) {
        JdbcUtils.createDataSource(dataSourceConfig).use { dataSource ->
            dataSource.connection.use { connection ->
                // PostgreSQL requires autocommit to be disabled to enable cursor streaming
                // see https://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor
                connection.autoCommit = false
                connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { ps ->
                    ps.fetchSize = fetchSize
                    LOGGER.info("Executing query: {}", query)
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