package me.jayer.hdata.jdbc

import me.jayer.hdata.core.spi.StructuredSource
import me.jayer.hdata.jdbc.handler.RowHandler
import me.jayer.hdata.jdbc.partition.PartitionConverter
import me.jayer.hdata.jdbc.partition.PartitionConverters
import me.jayer.hdata.jdbc.statement.SelectStatement
import me.jayer.hdata.jdbc.transform.JdbcSourceDoFn
import me.jayer.hdata.jdbc.transform.JdbcSourceSplittableDoFn
import me.jayer.hdata.jdbc.type.JdbcTypeRegistry
import me.jayer.hdata.jdbc.util.JdbcUtils
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
class JdbcStructuredSource(private val sourceDescriptor: me.jayer.hdata.jdbc.JdbcSourceDescriptor) : StructuredSource() {

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(me.jayer.hdata.jdbc.JdbcStructuredSource::class.java)
    }

    override fun expand(input: PBegin): PCollection<Row> {
        var (dataSourceConfig, columns, tables, where, partitionColumn, partitionNum, query, fetchSize) = sourceDescriptor
        me.jayer.hdata.jdbc.util.JdbcUtils.createDataSource(dataSourceConfig).use { dataSource ->
            dataSource.connection.use { connection ->
                return if (query.isNotBlank()) {
                    val columnMetas = me.jayer.hdata.jdbc.util.JdbcUtils.getQuerySchema(connection, query)
                    val schema = me.jayer.hdata.jdbc.util.JdbcUtils.inferBeamSchema(columnMetas)
                    val resultSetGetters = columnMetas.map { me.jayer.hdata.jdbc.type.JdbcTypeRegistry.getResultSetGetter(it)!! }
                    val rowHandler = me.jayer.hdata.jdbc.handler.RowHandler(schema, resultSetGetters)
                    input.apply(Create.of(query))
                        .apply("Jdbc Source", ParDo.of(
                            me.jayer.hdata.jdbc.transform.JdbcSourceDoFn(
                                dataSourceConfig,
                                fetchSize,
                                rowHandler
                            )
                        ))
                        .setRowSchema(schema)
                } else {
                    val resolvedTables = me.jayer.hdata.jdbc.util.JdbcUtils.resolveTables(tables)
                    val table = resolvedTables[0]
                    var partitionConverter: me.jayer.hdata.jdbc.partition.PartitionConverter<out Any>? = null
                    if (partitionNum == null || partitionNum > 1) {
                        val tableSchema = me.jayer.hdata.jdbc.util.JdbcUtils.getTableSchema(connection, table)
                        if (partitionColumn.isBlank()) {
                            val primaryKey = me.jayer.hdata.jdbc.util.JdbcUtils.getPrimaryKeys(connection, table).firstOrNull()
                            if (primaryKey != null) {
                                partitionConverter = getPartitionConverter(getColumnMeta(tableSchema, primaryKey)!!)
                                if (partitionConverter != null) {
                                    partitionColumn = primaryKey
                                    me.jayer.hdata.jdbc.JdbcStructuredSource.Companion.LOGGER.info("PartitionColumn is not specified, automatically found: $primaryKey")
                                }
                            }
                        } else {
                            val columnMeta = getColumnMeta(tableSchema, partitionColumn)
                            requireNotNull(columnMeta) { "Unknown Partition column[$partitionColumn], possible columns are: ${tableSchema.map { it.label }}" }

                            partitionConverter = getPartitionConverter(columnMeta)
                            requireNotNull(partitionConverter) {
                                "Unsupported partition column type[${columnMeta.typeName}], class[${Class.forName(columnMeta.typeClass).canonicalName}]: $partitionColumn, supported partition column types are: ${
                                    me.jayer.hdata.jdbc.partition.PartitionConverters.values().map { it.type.javaObjectType.canonicalName }
                                }"
                            }
                        }
                    }

                    val columnMetas =
                        me.jayer.hdata.jdbc.util.JdbcUtils.getQuerySchema(connection, me.jayer.hdata.jdbc.statement.SelectStatement(
                            columns,
                            table,
                            listOf(where)
                        ).buildSql())
                    val schema = me.jayer.hdata.jdbc.util.JdbcUtils.inferBeamSchema(columnMetas)
                    val resultSetGetters = columnMetas.map { me.jayer.hdata.jdbc.type.JdbcTypeRegistry.getResultSetGetter(it)!! }
                    val rowHandler = me.jayer.hdata.jdbc.handler.RowHandler(schema, resultSetGetters)
                    val statements = resolvedTables.map {
                        me.jayer.hdata.jdbc.statement.SelectStatement(
                            columns,
                            it,
                            listOf(where)
                        )
                    }
                    if (partitionConverter == null) {
                        input.apply(Create.of(statements.map { it.buildSql() }))
                            .apply("Jdbc Source", ParDo.of(
                                me.jayer.hdata.jdbc.transform.JdbcSourceDoFn(
                                    dataSourceConfig,
                                    fetchSize,
                                    rowHandler
                                )
                            ))
                            .setRowSchema(schema)
                    } else {
                        input.apply(Create.of(statements)).apply(
                            "Jdbc Splittable Source", ParDo.of(
                                me.jayer.hdata.jdbc.transform.JdbcSourceSplittableDoFn(
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

    private fun getColumnMeta(tableSchema: List<me.jayer.hdata.jdbc.JdbcColumnMeta>, column: String): me.jayer.hdata.jdbc.JdbcColumnMeta? {
        return tableSchema.firstOrNull { it.label == column }
    }

    private fun getPartitionConverter(columnMeta: me.jayer.hdata.jdbc.JdbcColumnMeta): me.jayer.hdata.jdbc.partition.PartitionConverter<out Any>? {
        return me.jayer.hdata.jdbc.partition.PartitionConverters.values()
            .filter { it.type == Class.forName(columnMeta.typeClass).kotlin }
            .map { it.partitionConverter }
            .firstOrNull()
    }
}