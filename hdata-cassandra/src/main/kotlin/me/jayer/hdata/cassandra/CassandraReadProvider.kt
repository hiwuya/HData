package me.jayer.hdata.cassandra

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.cassandra.internal.CassandraSessions
import me.jayer.hdata.cassandra.internal.CassandraTypeMappings
import me.jayer.hdata.cassandra.transform.CassandraReadFn
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import com.datastax.oss.driver.api.core.metadata.token.TokenRange

/**
 * `ReadFromCassandra`: executes a CQL SELECT query against a Cassandra cluster and streams the
 * result rows.
 *
 * The output schema is not known statically — it depends on the query — so `create()` connects to
 * Cassandra once at graph-construction time and prepares the query to obtain its column metadata
 * (`PreparedStatement.getResultSetDefinitions()`; no rows are actually read) to give the output
 * `PCollection` a schema/coder. Without this, Beam cannot serialize the output and every
 * downstream transform fails with "Unable to return a default Coder for a Beam Row" at
 * graph-finalization time. This means `ReadFromCassandra` needs the real cluster reachable at
 * graph-construction time (`--dryRun` included), unlike most other connectors.
 *
 * @author wuya
 */
class CassandraReadProvider : TypedTransformProvider<CassandraReadConfig>(CassandraReadConfig::class.java) {

    override fun identifier(): String = "ReadFromCassandra"

    override fun description(): String = "Read from Cassandra by executing a CQL SELECT query"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(
        config: CassandraReadConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        val schema = probeSchema(config)
        val ranges = if (config.parallelScanSegments > 1) tokenRanges(config) else emptyList()
        return CassandraSource(config, schema, ranges)
    }

    private fun probeSchema(config: CassandraReadConfig): Schema {
        try {
            CassandraSessions.newSession(
                config.endpoints, config.keyspace, config.datacenter,
                config.connectTimeoutMs, config.requestTimeoutMs,
            ).use { session ->
                val prepared = session.prepare(config.query)
                return CassandraTypeMappings.deriveSchema(prepared.resultSetDefinitions)
            }
        } catch (e: Exception) {
            throw HDataException("ReadFromCassandra could not determine the output schema: ${e.message}", e)
        }
    }

    private fun tokenRanges(config: CassandraReadConfig): List<CassandraTokenRange> {
        try {
            CassandraSessions.newSession(
                config.endpoints, config.keyspace, config.datacenter,
                config.connectTimeoutMs, config.requestTimeoutMs,
            ).use { session ->
                val tokenMap = session.metadata.tokenMap.orElseThrow {
                    IllegalStateException("Cassandra token metadata is unavailable")
                }
                return tokenMap.getTokenRanges().flatMap { range: TokenRange ->
                    range.unwrap().flatMap { unwrapped ->
                        unwrapped.splitEvenly(config.parallelScanSegments).map {
                            CassandraTokenRange(tokenMap.format(it.start), tokenMap.format(it.end))
                        }
                    }
                }
            }
        } catch (e: Exception) {
            throw HDataException("ReadFromCassandra could not determine token ranges: ${e.message}", e)
        }
    }
}

private class CassandraSource(
    private val config: CassandraReadConfig,
    private val schema: Schema,
    private val ranges: List<CassandraTokenRange>,
) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        val triggers = if (ranges.isEmpty()) listOf(CassandraTokenRange(null, null)) else ranges
        return begin
            .apply("Trigger", Create.of(triggers))
            .apply("ReadFromCassandra", ParDo.of(CassandraReadFn(config)))
            .setRowSchema(schema)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

data class CassandraTokenRange(val start: String?, val end: String?) : java.io.Serializable
