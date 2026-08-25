package me.jayer.hdata.jdbc

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.jdbc.internal.DataSources
import me.jayer.hdata.jdbc.internal.JdbcMetadata
import me.jayer.hdata.jdbc.internal.RowMapper
import me.jayer.hdata.jdbc.internal.SelectSql
import me.jayer.hdata.jdbc.internal.TableNames
import me.jayer.hdata.jdbc.internal.parseJdbcAggregations
import me.jayer.hdata.jdbc.internal.renderAggregateSelect
import me.jayer.hdata.jdbc.partition.PartitionColumn
import me.jayer.hdata.jdbc.partition.PartitionColumns
import me.jayer.hdata.jdbc.transform.JdbcPartitionedReadFn
import me.jayer.hdata.jdbc.transform.JdbcQueryReadFn
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import java.sql.Connection

/**
 * `ReadFromJdbc`：从关系库读出带 schema 的行。
 *
 * schema 与分区方案都在**构图阶段**通过元数据查询确定，因此提交作业的机器需要能连上库。
 *
 * @author wuya
 * @date 2022-08-05
 */
class JdbcReadProvider : TypedTransformProvider<JdbcReadConfig>(JdbcReadConfig::class.java) {

    override fun identifier(): String = "ReadFromJdbc"

    override fun description(): String = "按表或自定义 SQL 从关系库读取数据，支持按列分区并行读"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(
        config: JdbcReadConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        DataSources.warnIfMySqlWithoutCursor(config.url, config.fetchSize)
        return JdbcSource(config)
    }
}

private class JdbcSource(private val config: JdbcReadConfig) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> =
        DataSources.withConnection(config.dataSourceProperties(), "hdata-jdbc-metadata") { connection ->
            if (config.aggregations.isNotEmpty()) readAggregate(begin, connection)
            else if (config.query.isNotBlank()) readQuery(begin, connection) else readTables(begin, connection)
        }

    private fun readAggregate(begin: PBegin, connection: Connection): PCollection<Row> {
        // 聚合下推：把 aggregations 翻译成 DB 原生聚合 SQL（带别名），在数据源侧算完返回单行
        val specs = parseJdbcAggregations(config.aggregations)
        val aggSelect = renderAggregateSelect(specs)
        val from = if (config.query.isNotBlank()) "FROM (${config.query}) hdata_sub" else "FROM ${config.tables.first()}"
        val where = if (config.where.isNotBlank()) " WHERE (${config.where})" else ""
        val sql = "SELECT $aggSelect $from$where"
        val plan = planOf(connection, sql)
        return begin.apply("Aggregate", Create.of(sql))
            .apply("Read", ParDo.of(JdbcQueryReadFn(config.dataSourceProperties(), config.fetchSize, plan.mapper)))
            .setRowSchema(plan.schema)
    }

    private fun readQuery(begin: PBegin, connection: Connection): PCollection<Row> {
        val plan = planOf(connection, config.query)
        return begin.apply("Query", Create.of(config.query))
            .apply("Read", ParDo.of(JdbcQueryReadFn(config.dataSourceProperties(), config.fetchSize, plan.mapper)))
            .setRowSchema(plan.schema)
    }

    private fun readTables(begin: PBegin, connection: Connection): PCollection<Row> {
        val tables = TableNames.resolve(config.tables)
        val selects = tables.map { SelectSql(it, config.columns, listOf(config.where), config.limit) }
        // 多表同步的前提是它们结构一致，schema 与分区列都按第一张表确定
        val plan = planOf(connection, selects.first().render())
        // LIMIT 必须是全局的：SQL 的 LIMIT 只作用于单条语句，分区读会把它变成"每片 LIMIT"，
        // 所以限制了行数时直接退化为单分区读，让 LIMIT 在库侧对整个结果集生效（此时不自动探测主键分区）。
        val partitionColumn = if (config.limit <= 0) {
            resolvePartitionColumn(connection, tables.first(), selects.first(), plan)
        } else {
            null
        }

        if (partitionColumn == null) {
            return begin.apply("Statements", Create.of(selects.map { it.render() }))
                .apply("Read", ParDo.of(JdbcQueryReadFn(config.dataSourceProperties(), config.fetchSize, plan.mapper)))
                .setRowSchema(plan.schema)
        }
        return begin.apply("Statements", Create.of(selects))
            .apply(
                "PartitionedRead",
                ParDo.of(
                    JdbcPartitionedReadFn(
                        config.dataSourceProperties(),
                        partitionColumn,
                        config.partitionNum,
                        config.fetchSize,
                        plan.mapper,
                    )
                )
            )
            .setRowSchema(plan.schema)
    }

    private fun resolvePartitionColumn(
        connection: Connection,
        table: String,
        probe: SelectSql,
        plan: ReadPlan,
    ): PartitionColumn? {
        if (config.partitionNum != null && config.partitionNum <= 1) {
            return null
        }
        return PartitionColumns.resolve(connection, table, plan.columns, config.partitionColumn, probe)
    }

    private fun planOf(connection: Connection, sql: String): ReadPlan {
        val columns = JdbcMetadata.describe(connection, sql)
        val (schema, readers) = JdbcMetadata.toSchema(columns)
        return ReadPlan(columns, schema, RowMapper(schema, readers))
    }

    private class ReadPlan(
        val columns: List<me.jayer.hdata.jdbc.internal.JdbcColumn>,
        val schema: org.apache.beam.sdk.schemas.Schema,
        val mapper: RowMapper,
    )

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
