package io.jayer.hdata.jdbc

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.jdbc.JdbcIO
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.values.Row
import java.sql.PreparedStatement
import java.util.*


/**
 * @author wuya
 * @date 2022-07-27
 */
object BeamDemo {

    @JvmStatic
    fun main(args: Array<String>) {
        val dataSourceConfiguration = DataSourceConfiguration.create(
            "com.mysql.cj.jdbc.Driver", "jdbc:mysql://10.247.17.31:3306/wuya_test?useSSL=false"
        ).withUsername("51generalnew").withPassword("uUZPeL32FpatOvju")

        val dataSourceConfig = Properties().apply {
            setProperty("jdbcUrl", "jdbc:mysql://10.247.17.31:3306/wuya_test?useSSL=false")
            setProperty("dataSource.user", "51generalnew")
            setProperty("dataSource.password", "uUZPeL32FpatOvju")
        }

        val sourceDescriptor = JdbcSourceDescriptor(
            dataSourceConfig,
            table = "T_Test_1",
            fetchSize = 50000
        )

        val options = PipelineOptionsFactory.fromArgs(*args).create()
        options.jobName = "Beam Test"
        val pipeline: Pipeline = Pipeline.create(options)
        val source = pipeline.apply(JdbcStructuredSource(sourceDescriptor))

        source.apply(JdbcIO.write<Row>().withDataSourceConfiguration(dataSourceConfiguration)
            .withStatement("INSERT INTO T_Test_2 (AutoId, Name) VALUES (?, ?)")
            .withPreparedStatementSetter { row: Row, query: PreparedStatement ->
                query.setObject(1, row.getValue(0)!!)
                query.setObject(2, row.getValue(1))
            })

        pipeline.run().waitUntilFinish()
    }
}