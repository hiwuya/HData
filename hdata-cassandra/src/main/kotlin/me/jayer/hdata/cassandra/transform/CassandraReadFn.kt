package me.jayer.hdata.cassandra.transform

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.ResultSet
import com.datastax.oss.driver.api.core.cql.Row
import me.jayer.hdata.cassandra.CassandraReadConfig
import me.jayer.hdata.cassandra.CassandraTokenRange
import me.jayer.hdata.cassandra.internal.CassandraSessions
import me.jayer.hdata.cassandra.internal.CassandraTypeMappings
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime

/**
 * Executes a CQL SELECT query against Cassandra and converts each result row into a Beam [Row].
 *
 * The output schema is derived from the result set metadata at runtime. This is a plain DoFn
 * (not an SDF): parallelism comes from the number of input elements. The read provider supplies
 * a single trigger element, so the query runs exactly once.
 *
 * @author wuya
 */
class CassandraReadFn(
    private val config: CassandraReadConfig,
) : DoFn<CassandraTokenRange, org.apache.beam.sdk.values.Row>() {

    @Transient
    private var session: CqlSession? = null

    @Setup
    fun setup() {
        session = CassandraSessions.newSession(
            config.endpoints, config.keyspace, config.datacenter,
            config.connectTimeoutMs, config.requestTimeoutMs,
        )
    }

    @Teardown
    fun tearDown() {
        runCatching { session?.close() }
        session = null
    }

    @ProcessElement
    fun processElement(@Element range: CassandraTokenRange, context: ProcessContext) {
        val s = checkNotNull(session) { "Cassandra session is not initialized" }

        val query = if (range.start == null) config.query else {
            require(!config.query.contains(';')) { "token-range query must not contain a semicolon" }
            val predicate = "token(${config.partitionKeyColumn}) > ${range.start} AND token(${config.partitionKeyColumn}) <= ${range.end}"
            addTokenPredicate(config.query.trim(), predicate)
        }
        val stmtBuilder = com.datastax.oss.driver.api.core.cql.SimpleStatement.newInstance(query)
            .setConsistencyLevel(
                com.datastax.oss.driver.api.core.DefaultConsistencyLevel.valueOf(config.consistencyLevel)
            )
            .setFetchSize(config.fetchSize)

        val rs: ResultSet = s.execute(stmtBuilder)
        val columnDefs = rs.columnDefinitions

        // Derive Beam schema from column metadata — same logic the provider used at graph
        // construction time to set the PCollection's schema/coder (deterministic for a given
        // query, so the two stay in sync).
        val schema = CassandraTypeMappings.deriveSchema(columnDefs)

        var count = 0L
        val maxRows = if (config.maxRows > 0) config.maxRows.toLong() else Long.MAX_VALUE

        for (row in rs) {
            if (count >= maxRows) break
            val beamRow = convertRow(row, schema)
            context.output(beamRow)
            count++
        }

        LOGGER.info("Read {} rows from Cassandra", count)
    }

    private fun addTokenPredicate(query: String, predicate: String): String {
        val clause = Regex("\\b(order\\s+by|group\\s+by|limit|allow\\s+filtering|per\\s+partition\\s+limit)\\b", RegexOption.IGNORE_CASE)
        val match = clause.find(query)
        val head = match?.let { query.substring(0, it.range.first).trimEnd() } ?: query
        val tail = match?.let { " ${query.substring(it.range.first).trimStart()}" } ?: ""
        val withPredicate = if (head.contains(Regex("\\bwhere\\b", RegexOption.IGNORE_CASE))) {
            "$head AND $predicate"
        } else {
            "$head WHERE $predicate"
        }
        return withPredicate + tail
    }

    private fun convertRow(row: Row, schema: Schema): org.apache.beam.sdk.values.Row {
        val builder = org.apache.beam.sdk.values.Row.withSchema(schema)
        for (i in 0 until schema.fieldCount) {
            val colName = schema.getField(i).name
            val fieldType = schema.getField(i).type
            val value = row.getObject(i)
            builder.addValue(convertValue(value, fieldType))
        }
        return builder.build()
    }

    private fun convertValue(value: Any?, fieldType: Schema.FieldType): Any? {
        if (value == null) return null
        return when (fieldType.typeName) {
            Schema.TypeName.STRING -> value.toString()
            Schema.TypeName.BYTE -> (value as? Number)?.toByte()
            Schema.TypeName.INT16 -> (value as? Number)?.toShort()
            Schema.TypeName.INT32 -> (value as? Number)?.toInt()
            Schema.TypeName.INT64 -> (value as? Number)?.toLong()
            Schema.TypeName.FLOAT -> (value as? Number)?.toFloat()
            Schema.TypeName.DOUBLE -> (value as? Number)?.toDouble()
            Schema.TypeName.BOOLEAN -> {
                when (value) {
                    is Boolean -> value
                    is Number -> value.toInt() != 0
                    else -> value.toString().toBooleanStrictOrNull()
                }
            }
            Schema.TypeName.DECIMAL -> when (value) {
                is java.math.BigDecimal -> value
                is Number -> java.math.BigDecimal.valueOf(value.toDouble())
                else -> runCatching { java.math.BigDecimal(value.toString()) }.getOrNull()
            }
            Schema.TypeName.DATETIME -> when (value) {
                is Instant -> LocalDateTime.ofInstant(value, java.time.ZoneOffset.UTC)
                is java.sql.Timestamp -> value.toLocalDateTime()
                is LocalDate -> value.atStartOfDay()
                is LocalDateTime -> value
                is OffsetDateTime -> value.toLocalDateTime()
                is java.util.Date -> LocalDateTime.ofInstant(value.toInstant(), java.time.ZoneOffset.UTC)
                else -> runCatching { LocalDateTime.parse(value.toString()) }.getOrNull()
            }
            Schema.TypeName.BYTES -> when (value) {
                is java.nio.ByteBuffer -> {
                    val bytes = ByteArray(value.remaining())
                    value.get(bytes)
                    bytes
                }
                is ByteArray -> value
                is String -> value.toByteArray(Charsets.UTF_8)
                else -> value.toString().toByteArray(Charsets.UTF_8)
            }
            Schema.TypeName.ARRAY -> {
                when (value) {
                    is Collection<*> -> value.toList()
                    is Array<*> -> value.toList()
                    else -> listOf(value.toString())
                }
            }
            Schema.TypeName.MAP -> {
                when (value) {
                    is Map<*, *> -> value
                    else -> emptyMap<Any, Any>()
                }
            }
            else -> value.toString()
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(CassandraReadFn::class.java)
    }
}
