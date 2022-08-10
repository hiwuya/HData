package io.jayer.hdata.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
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
    private val rowMapper: BeamRowMapper,
    private val dataSourceConfig: Properties,
    private val query: String,
    private val fetchSize: Int,
) : DoFn<String, Row>() {

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(JdbcSourceDoFn::class.java)
    }

    @ProcessElement
    fun processElement(receiver: OutputReceiver<Row>) {
        HikariDataSource(HikariConfig(dataSourceConfig)).use { dataSource ->
            dataSource.connection.use { connection ->
                val ps = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
                ps.fetchSize = fetchSize
                ps.use {
                    LOGGER.info("Executing query: {}", query)
                    it.executeQuery().use { rs ->
                        while (rs.next()) {
                            receiver.output(rowMapper.mapRow(rs))
                        }
                    }
                }
            }
        }
    }
}