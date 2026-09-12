package me.jayer.hdata.jdbc

import me.jayer.hdata.core.spi.DeliveryCapabilities
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.OrderingScope
import me.jayer.hdata.core.spi.ReplayBehavior
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
 * `ReadFromJdbc`: reads rows with a schema out of a relational database.
 *
 * Both the schema and the partitioning plan are determined at **graph construction time** via metadata queries, so the machine submitting the job must be able to reach the database.
 *
 * @author wuya
 * @date 2022-08-05
 */
class JdbcReadProvider : TypedTransformProvider<JdbcReadConfig>(JdbcReadConfig::class.java) {

    override fun identifier(): String = "ReadFromJdbc"

    override fun description(): String = "Reads data from a relational database by table or custom SQL, with parallel reads partitioned by column"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun deliveryCapabilities(config: TransformConfig): DeliveryCapabilities = DeliveryCapabilities(
        deliveryMode = DeliveryMode.AT_LEAST_ONCE,
        replayBehavior = ReplayBehavior.FULL_REPLAY,
        ordering = OrderingScope.NONE,
        notes = "Bounded batch read with no position persisted outside the job; a full job restart re-reads " +
            "every row from scratch. Safe only if the read is a stable snapshot (the table is not concurrently " +
            "modified) or downstream tolerates re-reading current data. Partitioned parallel reads carry no row order.",
    )

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
        // Push-down aggregation: translate aggregations into native DB aggregate SQL (with aliases), evaluated on the source side and returned as a single row
        val specs = parseJdbcAggregations(config.aggregations)
        val aggSelect = renderAggregateSelect(specs)
        val from = if (config.query.isNotBlank()) {
            "FROM (${config.query}) hdata_sub"
        } else {
            "FROM ${TableNames.resolve(config.tables).single()}"
        }
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
        // Syncing multiple tables assumes they share the same structure; the schema and the partition column are both taken from the first table
        val plan = planOf(connection, selects.first().render())
        // LIMIT must be global: an SQL LIMIT only applies to a single statement, and a partitioned read would turn it into "LIMIT per chunk",
        // so when a row limit is set we degrade to a single-partition read, letting LIMIT apply to the whole result set on the database side, and primary key partition detection is skipped.
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
