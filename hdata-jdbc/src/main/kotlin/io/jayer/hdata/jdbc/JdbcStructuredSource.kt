package io.jayer.hdata.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.jayer.hdata.core.StructuredSource
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory

/**
 * @author wuya
 * @date 2022-08-05
 */
class JdbcStructuredSource(private val sourceDescriptor: JdbcSourceDescriptor) : StructuredSource() {

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(JdbcStructuredSource::class.java)
    }

    override fun expand(input: PBegin): PCollection<Row> {
        val table = sourceDescriptor.table
        var query = sourceDescriptor.query
        val fetchSize = sourceDescriptor.fetchSize
        var partitionColumn: String? = sourceDescriptor.partitionColumn

        require(table.isNotBlank() || query.isNotBlank()) { "table or query is required" }
        require(fetchSize > 0) { "fetchSize is required > 0" }

        HikariDataSource(HikariConfig(sourceDescriptor.dataSourceConfig)).use { dataSource ->
            if (query.isBlank()) {
                query = sourceDescriptor.createSchemaQuery()
                if (partitionColumn.isNullOrBlank()) {
                    partitionColumn = dataSource.connection.use { connection ->
                        LOGGER.info("PartitionColumn is not specified, try to find first primary key of numeric type......")
                        val result = JdbcUtils.getFirstNumericPrimaryKey(connection, table)
                        if (result.isNullOrBlank()) {
                            LOGGER.info("PartitionColumn not found")
                        } else {
                            LOGGER.info("PartitionColumn found: {}", result)
                        }
                        result
                    }
                }

                if (!partitionColumn.isNullOrBlank()) {
                    // read via SDF
                    val schema = dataSource.connection.use { connection ->
                        JdbcUtils.inferBeamSchema(connection, query)
                    }
                    val rowMapper = BeamRowMapper(schema)
                    val sdf = JdbcSourceSplittableDoFn(
                        rowMapper = rowMapper,
                        dataSourceConfig = sourceDescriptor.dataSourceConfig,
                        columns = sourceDescriptor.columns,
                        where = sourceDescriptor.where,
                        partitionColumn = partitionColumn!!,
                        fetchSize = fetchSize
                    )
                    return input.apply(Create.of(table))
                        .apply("Jdbc Splittable Source", ParDo.of(sdf))
                        .setRowSchema(schema)
                }
            }

            // read via DoFn
            val schema = dataSource.connection.use { connection ->
                JdbcUtils.inferBeamSchema(connection, query)
            }
            val rowMapper = BeamRowMapper(schema)
            val doFn = JdbcSourceDoFn(
                rowMapper = rowMapper,
                dataSourceConfig = sourceDescriptor.dataSourceConfig,
                query = query,
                fetchSize = fetchSize
            )
            return input.apply(Create.of(table)).apply("Jdbc Source", ParDo.of(doFn)).setRowSchema(schema)
        }
    }
}