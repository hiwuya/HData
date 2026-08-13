package me.jayer.hdata.jdbc.transform

import com.zaxxer.hikari.HikariDataSource
import me.jayer.hdata.jdbc.internal.DataSources
import me.jayer.hdata.jdbc.internal.RowMapper
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.util.Properties

/**
 * 一条 SQL 读一批行。
 *
 * 连接池在 `@Setup` 建、`@Teardown` 关；重构前是**每来一个元素**新建一个连接池再销毁，
 * 多表同步时等于把建池/销毁的开销乘上了表数量。
 *
 * @author wuya
 * @date 2022-07-27
 */
class JdbcQueryReadFn(
    private val dataSourceProperties: Properties,
    private val fetchSize: Int,
    private val rowMapper: RowMapper,
) : DoFn<String, Row>() {

    @Transient
    private var dataSource: HikariDataSource? = null

    @Setup
    fun setup() {
        dataSource = DataSources.create(dataSourceProperties, "hdata-jdbc-read")
    }

    @Teardown
    fun tearDown() {
        dataSource?.close()
        dataSource = null
    }

    @ProcessElement
    fun processElement(@Element sql: String, receiver: OutputReceiver<Row>) {
        val pool = checkNotNull(dataSource) { "数据源未初始化" }
        pool.connection.use { connection ->
            // PostgreSQL 必须关掉 autocommit 才会走游标流式读取
            // https://jdbc.postgresql.org/documentation/query/#getting-results-based-on-a-cursor
            connection.autoCommit = false
            connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { ps ->
                ps.fetchSize = fetchSize
                LOGGER.info("Executing query: {}", sql)
                var count = 0L
                ps.executeQuery().use { rs ->
                    while (rs.next()) {
                        receiver.output(rowMapper.map(rs))
                        count++
                    }
                }
                RECORDS_READ.inc(count)
                LOGGER.info("Read {} rows from: {}", count, sql)
            }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(JdbcQueryReadFn::class.java)
        private val RECORDS_READ = Metrics.counter(JdbcQueryReadFn::class.java, "records_read")
    }
}
