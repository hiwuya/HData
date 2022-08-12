package io.jayer.hdata.jdbc

import io.jayer.hdata.core.StructuredSource
import io.jayer.hdata.jdbc.handler.RowHandler
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
        sourceDescriptor.validate()

        var (dataSourceConfig, _, table, _, partitionColumn, partitionNum, query, _) = sourceDescriptor
        JdbcUtils.createDataSource(dataSourceConfig).use { dataSource ->
            dataSource.connection.use { connection ->
                if (query.isBlank() && partitionColumn.isBlank() && (partitionNum == null || partitionNum > 1)) {
                    LOGGER.info("PartitionColumn is not specified for table[$table], try to find primary key of numeric type...")
                    partitionColumn = JdbcUtils.getNumericPrimaryKey(connection, table) ?: ""
                    if (partitionColumn.isBlank()) {
                        LOGGER.info("Primary key of numeric type not found for table[$table]")
                    } else {
                        LOGGER.info("Primary key of numeric type found for table[$table]: $partitionColumn")
                    }
                }

                val schema = JdbcUtils.inferBeamSchema(connection, sourceDescriptor.createSchemaQuery())
                val rowHandler = RowHandler(schema)
                val sdf = JdbcSourceSplittableDoFn(rowHandler)
                return input.apply(Create.of(sourceDescriptor.copy(partitionColumn = partitionColumn)))
                    .apply("Jdbc Splittable Source", ParDo.of(sdf))
                    .setRowSchema(schema)
            }
        }
    }
}