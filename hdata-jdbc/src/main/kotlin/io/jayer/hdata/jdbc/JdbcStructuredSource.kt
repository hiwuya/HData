package io.jayer.hdata.jdbc

import io.jayer.hdata.core.spi.StructuredSource
import io.jayer.hdata.jdbc.handler.RowHandler
import io.jayer.hdata.jdbc.partition.PartitionConverter
import io.jayer.hdata.jdbc.partition.PartitionConverters
import io.jayer.hdata.jdbc.statement.SelectStatement
import io.jayer.hdata.jdbc.transform.JdbcSourceDoFn
import io.jayer.hdata.jdbc.transform.JdbcSourceSplittableDoFn
import io.jayer.hdata.jdbc.type.JdbcTypeRegistry
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

        var (dataSourceConfig, columns, table, where, partitionColumn, partitionNum, query, fetchSize) = sourceDescriptor
        JdbcUtils.createDataSource(dataSourceConfig).use { dataSource ->
            dataSource.connection.use { connection ->
                var partitionConverter: PartitionConverter<out Any>? = null
                if (query.isBlank() && (partitionNum == null || partitionNum > 1)) {
                    if (partitionColumn.isBlank()) {
                        LOGGER.info("PartitionColumn is not specified for table[$table], try to find primary key of numeric type...")
                        partitionColumn = JdbcUtils.getNumericPrimaryKey(connection, table) ?: ""
                        if (partitionColumn.isBlank()) {
                            LOGGER.info("Primary key of numeric type not found for table[$table]")
                        } else {
                            LOGGER.info("Primary key of numeric type found for table[$table]: $partitionColumn")
                        }
                    }

                    if (partitionColumn.isNotBlank()) {
                        val column =
                            JdbcUtils.getTableSchema(connection, table).firstOrNull { it.label == partitionColumn }
                        requireNotNull(column) { "Unknown column[$partitionColumn] for table[$table]" }
                        partitionConverter =
                            PartitionConverters.values().filter { it.type == Class.forName(column.typeClass).kotlin }
                                .map { it.partitionConverter }.firstOrNull()
                        requireNotNull(partitionConverter) { "Unsupported partition column type[${column.typeName}], class[${column.typeClass}] for table[$table]: $partitionColumn" }
                    }
                }

                val sql = query.ifBlank { SelectStatement(columns, table, listOf(where)).buildSql() }
                val columnMetas = JdbcUtils.getQuerySchema(connection, sql)
                val schema = JdbcUtils.inferBeamSchema(columnMetas)
                val resultSetGetters = columnMetas.map { JdbcTypeRegistry.getResultSetGetter(it)!! }
                val rowHandler = RowHandler(schema, resultSetGetters)

                return input.apply(Create.of(null as Void?)).run {
                    if (partitionConverter != null) {
                        this.apply(
                            "Jdbc Splittable Source",
                            ParDo.of(
                                JdbcSourceSplittableDoFn(
                                    dataSourceConfig,
                                    SelectStatement(columns, table, listOf(where)),
                                    partitionColumn,
                                    partitionNum,
                                    fetchSize,
                                    rowHandler,
                                    partitionConverter
                                )
                            )
                        )
                    } else {
                        this.apply(
                            "Jdbc Source",
                            ParDo.of(JdbcSourceDoFn(dataSourceConfig, sql, fetchSize, rowHandler))
                        )
                    }
                }.setRowSchema(schema)
            }
        }
    }
}