package io.jayer.hdata.jdbc

import io.jayer.hdata.core.spi.StructuredSource
import io.jayer.hdata.jdbc.handler.RowHandler
import io.jayer.hdata.jdbc.partition.PartitionConverter
import io.jayer.hdata.jdbc.partition.PartitionConverters
import io.jayer.hdata.jdbc.statement.SelectStatement
import io.jayer.hdata.jdbc.transform.JdbcSourceDoFn
import io.jayer.hdata.jdbc.transform.JdbcSourceSplittableDoFn
import io.jayer.hdata.jdbc.type.JdbcTypeRegistry
import io.jayer.hdata.jdbc.util.JdbcUtils
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
        var (dataSourceConfig, columns, tables, where, partitionColumn, partitionNum, query, fetchSize) = sourceDescriptor
        JdbcUtils.createDataSource(dataSourceConfig).use { dataSource ->
            dataSource.connection.use { connection ->
                return if (query.isNotBlank()) {
                    val columnMetas = JdbcUtils.getQuerySchema(connection, query)
                    val schema = JdbcUtils.inferBeamSchema(columnMetas)
                    val resultSetGetters = columnMetas.map { JdbcTypeRegistry.getResultSetGetter(it)!! }
                    val rowHandler = RowHandler(schema, resultSetGetters)
                    input.apply(Create.of(query))
                        .apply("Jdbc Source", ParDo.of(JdbcSourceDoFn(dataSourceConfig, fetchSize, rowHandler)))
                        .setRowSchema(schema)
                } else {
                    val resolvedTables = JdbcUtils.resolveTables(tables)
                    val table = resolvedTables[0]
                    var partitionConverter: PartitionConverter<out Any>? = null
                    if (partitionNum == null || partitionNum > 1) {
                        val tableSchema = JdbcUtils.getTableSchema(connection, table)
                        if (partitionColumn.isBlank()) {
                            val primaryKey = JdbcUtils.getPrimaryKeys(connection, table).firstOrNull()
                            if (primaryKey != null) {
                                partitionConverter = getPartitionConverter(getColumnMeta(tableSchema, primaryKey)!!)
                                if (partitionConverter != null) {
                                    partitionColumn = primaryKey
                                    LOGGER.info("PartitionColumn is not specified, automatically found: $primaryKey")
                                }
                            }
                        } else {
                            val columnMeta = getColumnMeta(tableSchema, partitionColumn)
                            requireNotNull(columnMeta) { "Unknown Partition column[$partitionColumn], possible columns are: ${tableSchema.map { it.label }}" }

                            partitionConverter = getPartitionConverter(columnMeta)
                            requireNotNull(partitionConverter) {
                                "Unsupported partition column type[${columnMeta.typeName}], class[${Class.forName(columnMeta.typeClass).canonicalName}]: $partitionColumn, supported partition column types are: ${
                                    PartitionConverters.values().map { it.type.javaObjectType.canonicalName }
                                }"
                            }
                        }
                    }

                    val columnMetas =
                        JdbcUtils.getQuerySchema(connection, SelectStatement(columns, table, listOf(where)).buildSql())
                    val schema = JdbcUtils.inferBeamSchema(columnMetas)
                    val resultSetGetters = columnMetas.map { JdbcTypeRegistry.getResultSetGetter(it)!! }
                    val rowHandler = RowHandler(schema, resultSetGetters)
                    val statements = resolvedTables.map { SelectStatement(columns, it, listOf(where)) }
                    if (partitionConverter == null) {
                        input.apply(Create.of(statements.map { it.buildSql() }))
                            .apply("Jdbc Source", ParDo.of(JdbcSourceDoFn(dataSourceConfig, fetchSize, rowHandler)))
                            .setRowSchema(schema)
                    } else {
                        input.apply(Create.of(statements)).apply(
                            "Jdbc Splittable Source", ParDo.of(
                                JdbcSourceSplittableDoFn(
                                    dataSourceConfig,
                                    partitionColumn,
                                    partitionNum,
                                    fetchSize,
                                    rowHandler,
                                    partitionConverter
                                )
                            )
                        ).setRowSchema(schema)
                    }
                }
            }
        }
    }

    private fun getColumnMeta(tableSchema: List<JdbcColumnMeta>, column: String): JdbcColumnMeta? {
        return tableSchema.firstOrNull { it.label == column }
    }

    private fun getPartitionConverter(columnMeta: JdbcColumnMeta): PartitionConverter<out Any>? {
        return PartitionConverters.values()
            .filter { it.type == Class.forName(columnMeta.typeClass).kotlin }
            .map { it.partitionConverter }
            .firstOrNull()
    }
}