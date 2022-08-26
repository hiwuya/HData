package io.jayer.hdata.jdbc

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptionsFactory
import java.util.*


/**
 * @author wuya
 * @date 2022-07-27
 */
object BeamDemo {

    @JvmStatic
    fun main(args: Array<String>) {
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

        val sinkDescriptor = JdbcSinkDescriptor(dataSourceConfig, "T_Test_2", 3377)

        val options = PipelineOptionsFactory.fromArgs(*args).create()
        options.jobName = "Beam Test"
        val pipeline = Pipeline.create(options)
        pipeline.apply(JdbcStructuredSource(sourceDescriptor)).apply(JdbcStructuredSink(sinkDescriptor))
        pipeline.run().waitUntilFinish()
    }
}