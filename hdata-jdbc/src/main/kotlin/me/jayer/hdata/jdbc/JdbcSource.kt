package me.jayer.hdata.jdbc

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.jdbc.handler.RowHandler
import me.jayer.hdata.jdbc.partition.PartitionConverter
import me.jayer.hdata.jdbc.partition.PartitionConverters
import me.jayer.hdata.jdbc.statement.SelectStatement
import me.jayer.hdata.jdbc.transform.JdbcSourceDoFn
import me.jayer.hdata.jdbc.transform.JdbcSourceSplittableDoFn
import me.jayer.hdata.jdbc.type.JdbcTypeRegistry
import me.jayer.hdata.jdbc.util.JdbcUtils
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory
import java.sql.Connection

/**
 * `ReadFromJdbc`：从关系库读出带 schema 的行。
 *
 * schema 在构图阶段通过一次元数据查询推断，因此提交端需要能连上库。
 *
 * @author wuya
 * @date 2022-08-05
 */
class JdbcReadProvider : TypedTransformProvider<JdbcReadConfig>(JdbcReadConfig::class.java) {

    override fun identifier(): String = "ReadFromJdbc"

    override fun description(): String = "按表或自定义 SQL 从关系库读取数据，支持按列分区并行读"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(config: JdbcReadConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return JdbcSource(config)
    }
}

private class JdbcSource(private val config: JdbcReadConfig) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        val dataSourceConfig = config.dataSourceProperties()
        return JdbcUtils.createDataSource(dataSourceConfig).use { dataSource ->
            dataSource.connection.use { connection ->
                if (config.query.isNotBlank()) {
                    readQuery(begin, connection)
                } else {
                    readTables(begin, connection)
                }
            }
        }
    }

    private fun readQuery(begin: PBegin, connection: Connection): PCollection<Row> {
        val (schema, rowHandler) = resolveSchema(connection, config.query)
        return begin.apply(Create.of(config.query))
            .apply("Read", ParDo.of(JdbcSourceDoFn(config.dataSourceProperties(), config.fetchSize, rowHandler)))
            .setRowSchema(schema)
    }

    private fun readTables(begin: PBegin, connection: Connection): PCollection<Row> {
        val tables = JdbcUtils.resolveTables(config.tables)
        val statements = tables.map { SelectStatement(config.columns, it, listOf(config.where)) }
        val (schema, rowHandler) = resolveSchema(connection, statements.first().buildSql())

        val partitioning = resolvePartitioning(connection, tables.first())
        if (partitioning == null) {
            return begin.apply(Create.of(statements.map { it.buildSql() }))
                .apply("Read", ParDo.of(JdbcSourceDoFn(config.dataSourceProperties(), config.fetchSize, rowHandler)))
                .setRowSchema(schema)
        }

        val (partitionColumn, partitionConverter) = partitioning
        return begin.apply(Create.of(statements))
            .apply(
                "PartitionedRead",
                ParDo.of(
                    JdbcSourceSplittableDoFn(
                        config.dataSourceProperties(),
                        partitionColumn,
                        config.partitionNum,
                        config.fetchSize,
                        rowHandler,
                        partitionConverter,
                    )
                )
            )
            .setRowSchema(schema)
    }

    private fun resolveSchema(connection: Connection, sql: String): Pair<Schema, RowHandler> {
        val columnMetas = JdbcUtils.getQuerySchema(connection, sql)
        val schema = JdbcUtils.inferBeamSchema(columnMetas)
        val getters = columnMetas.map { meta ->
            requireNotNull(JdbcTypeRegistry.getResultSetGetter(meta)) {
                "列[${meta.label}] 的类型 ${meta.typeName}[${meta.typeClass}] 暂不支持"
            }
        }
        return schema to RowHandler(schema, getters)
    }

    /** @return 分区列与它的 long 映射；不分区时返回 null。 */
    private fun resolvePartitioning(connection: Connection, table: String): Pair<String, PartitionConverter<out Any>>? {
        if (config.partitionNum != null && config.partitionNum <= 1) {
            return null
        }
        val tableSchema = JdbcUtils.getTableSchema(connection, table)
        if (config.partitionColumn.isNotBlank()) {
            // 先精确匹配；H2 / PostgreSQL / Oracle 会把未加引号的列名统一大小写，所以再退一步忽略大小写
            val columnMeta = tableSchema.firstOrNull { it.label == config.partitionColumn }
                ?: tableSchema.firstOrNull { it.label.equals(config.partitionColumn, ignoreCase = true) }
            requireNotNull(columnMeta) {
                "未知的分区列[${config.partitionColumn}]，可选列: ${tableSchema.map { it.label }}"
            }
            val converter = partitionConverterOf(columnMeta)
            requireNotNull(converter) {
                "分区列[${config.partitionColumn}] 的类型 ${columnMeta.typeName}[${columnMeta.typeClass}] 不支持分区，" +
                    "支持的类型: ${PartitionConverters.entries.map { it.type.javaObjectType.canonicalName }}"
            }
            // 用库里实际的列名去拼 SQL
            return columnMeta.label to converter
        }

        // 没显式指定就拿主键试试
        val primaryKey = JdbcUtils.getPrimaryKeys(connection, table).firstOrNull() ?: return null
        val columnMeta = tableSchema.firstOrNull { it.label == primaryKey } ?: return null
        val converter = partitionConverterOf(columnMeta) ?: return null
        LOGGER.info("未指定 partition_column，自动使用主键: {}", primaryKey)
        return primaryKey to converter
    }

    private fun partitionConverterOf(columnMeta: JdbcColumnMeta): PartitionConverter<out Any>? =
        PartitionConverters.entries
            .firstOrNull { it.type == Class.forName(columnMeta.typeClass).kotlin }
            ?.partitionConverter

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(JdbcSource::class.java)
    }
}
