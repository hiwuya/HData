package me.jayer.hdata.clickhouse

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.clickhouse.internal.ClickHouseJdbc
import me.jayer.hdata.clickhouse.internal.ClickHouseTypeMappings
import me.jayer.hdata.clickhouse.transform.ClickHouseReadFn
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import java.sql.DriverManager

/**
 * `ReadFromClickHouse`: executes a SQL query against ClickHouse and streams the result rows.
 *
 * The output schema is not known statically — it depends on the query — so `create()` connects to
 * ClickHouse once at graph-construction time and probes the query's column metadata (via a
 * `LIMIT 0` wrapper, so no rows are actually transferred) to give the output `PCollection` a
 * schema/coder. Without this, Beam cannot serialize the output and every downstream transform
 * fails with "Unable to return a default Coder for a Beam Row" at graph-finalization time. This
 * means `ReadFromClickHouse` needs the real service reachable at graph-construction time (`--dryRun`
 * included), unlike most other connectors.
 *
 * @author wuya
 */
class ClickHouseReadProvider : TypedTransformProvider<ClickHouseReadConfig>(ClickHouseReadConfig::class.java) {

    override fun identifier(): String = "ReadFromClickHouse"

    override fun description(): String = "Read from ClickHouse by executing a SQL query"

    override fun supportTier(): ConnectorSupportTier = ConnectorSupportTier.EXPERIMENTAL

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(
        config: ClickHouseReadConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        val schema = probeSchema(config)
        return ClickHouseSource(config, schema)
    }

    private fun probeSchema(config: ClickHouseReadConfig): Schema {
        val jdbcUrl = ClickHouseJdbc.buildJdbcUrl(
            config.endpoint, config.database, config.connectTimeoutMs, config.socketTimeoutMs,
        )
        val probeSql = "SELECT * FROM (${config.query.trimEnd(';')}) AS _hdata_schema_probe LIMIT 0"
        try {
            DriverManager.getConnection(jdbcUrl, config.username, config.password).use { connection ->
                connection.createStatement().use { stmt ->
                    stmt.executeQuery(probeSql).use { rs ->
                        return ClickHouseTypeMappings.deriveSchema(rs.metaData)
                    }
                }
            }
        } catch (e: Exception) {
            throw HDataException("ReadFromClickHouse could not determine the output schema: ${e.message}", e)
        }
    }
}

private class ClickHouseSource(
    private val config: ClickHouseReadConfig,
    private val schema: Schema,
) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        return begin
            .apply("Trigger", Create.of(listOf(1)))
            .apply("ReadFromClickHouse", ParDo.of(ClickHouseReadFn(config)))
            .setRowSchema(schema)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
