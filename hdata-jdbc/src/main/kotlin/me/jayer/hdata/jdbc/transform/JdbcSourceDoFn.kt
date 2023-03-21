package me.jayer.hdata.jdbc.transform

import me.jayer.hdata.jdbc.handler.RowHandler
import me.jayer.hdata.jdbc.util.JdbcUtils
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
    private val fetchSize: Int,
    private val rowHandler: me.jayer.hdata.jdbc.handler.RowHandler,
) : DoFn<String, Row>() {

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(me.jayer.hdata.jdbc.transform.JdbcSourceDoFn::class.java)
    }

    @ProcessElement
    fun processElement(@Element query: String, receiver: OutputReceiver<Row>) {
        me.jayer.hdata.jdbc.util.JdbcUtils.createDataSource(dataSourceConfig).use { dataSource ->
            dataSource.connection.use { connection ->
                // PostgreSQL requires autocommit to be disabled to enable cursor streaming
                // see https://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor
                connection.autoCommit = false
                connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { ps ->
                    ps.fetchSize = fetchSize
                    me.jayer.hdata.jdbc.transform.JdbcSourceDoFn.Companion.LOGGER.info("Executing query: {}", query)
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