package me.jayer.hdata.clickhouse.transform

import me.jayer.hdata.clickhouse.ClickHouseReadConfig
import me.jayer.hdata.clickhouse.internal.ClickHouseJdbc
import me.jayer.hdata.clickhouse.internal.ClickHouseTypeMappings
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime

/**
 * Executes a SQL query against ClickHouse via JDBC and converts each result row into a Beam [Row].
 *
 * The output schema is derived from the result set metadata (column names and types) at runtime.
 * The query is one durable unit of work, represented by a single bounded SDF restriction. This
 * gives the runner a checkpoint boundary while avoiding a false claim that arbitrary JDBC result
 * rows can be split safely without an explicit partition key.
 *
 * @author wuya
 */
@DoFn.BoundedPerElement
class ClickHouseReadFn(
    private val config: ClickHouseReadConfig,
) : DoFn<Any, Row>() {

    @Transient
    private var connection: Connection? = null

    @Setup
    fun setup() {
        val jdbcUrl = ClickHouseJdbc.buildJdbcUrl(
            config.endpoint, config.database, config.connectTimeoutMs, config.socketTimeoutMs,
        )
        connection = DriverManager.getConnection(jdbcUrl, config.username, config.password)
        // ClickHouse has no transaction to commit/roll back, and the 0.9.x driver (client-v2)
        // actively rejects setAutoCommit(false) with SQLFeatureNotSupportedException.
    }

    @Teardown
    fun tearDown() {
        runCatching { connection?.close() }
        connection = null
    }

    @GetInitialRestriction
    fun getInitialRestriction(): OffsetRange = OffsetRange(0, 1)

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = OffsetRangeTracker(restriction)

    @ProcessElement
    fun processElement(
        @Element ignored: Any,
        tracker: RestrictionTracker<OffsetRange, Long>,
        output: OutputReceiver<Row>,
    ) {
        if (!tracker.tryClaim(0)) return
        val conn = checkNotNull(connection) { "ClickHouse connection is not initialized" }
        val sql = ClickHouseReadSql.withMaxRows(config.query, config.maxRows)

        LOGGER.info("Executing ClickHouse query: {}", sql)
        val stmt = conn.createStatement()
        try {
            stmt.fetchSize = 10_000
            val rs: ResultSet = stmt.executeQuery(sql)
            try {
                val metaData = rs.metaData
                val columnCount = metaData.columnCount

                // Derive Beam schema from JDBC metadata — same logic the provider used at graph
                // construction time to set the PCollection's schema/coder (ClickHouseTypeMappings
                // .deriveSchema is deterministic for a given query, so the two stay in sync).
                val schema = ClickHouseTypeMappings.deriveSchema(metaData)

                // Iterate over result rows.
                while (rs.next()) {
                    val row = convertRow(rs, schema, columnCount)
                    output.output(row)
                }
            } finally {
                rs.close()
            }
        } finally {
            stmt.close()
        }
    }

    private fun convertRow(rs: ResultSet, schema: Schema, columnCount: Int): Row {
        val builder = Row.withSchema(schema)
        for (i in 1..columnCount) {
            val fieldType = schema.getField(i - 1).type
            val value = convertValue(rs, i, fieldType)
            builder.addValue(value)
        }
        return builder.build()
    }

    private fun convertValue(rs: ResultSet, columnIndex: Int, fieldType: Schema.FieldType): Any? {
        val value = rs.getObject(columnIndex) ?: return null
        return when (fieldType.typeName) {
            Schema.TypeName.STRING -> value.toString()
            Schema.TypeName.BYTE -> (value as? Number)?.toByte() ?: value.toString().toByteOrNull()
            Schema.TypeName.INT16 -> (value as? Number)?.toShort() ?: value.toString().toShortOrNull()
            Schema.TypeName.INT32 -> (value as? Number)?.toInt() ?: value.toString().toIntOrNull()
            Schema.TypeName.INT64 -> (value as? Number)?.toLong() ?: value.toString().toLongOrNull()
            Schema.TypeName.FLOAT -> (value as? Number)?.toFloat() ?: value.toString().toFloatOrNull()
            Schema.TypeName.DOUBLE -> (value as? Number)?.toDouble() ?: value.toString().toDoubleOrNull()
            Schema.TypeName.BOOLEAN -> {
                when (value) {
                    is Boolean -> value
                    is Number -> value.toInt() != 0
                    else -> value.toString().toBooleanStrictOrNull()
                }
            }
            Schema.TypeName.DATETIME -> when (value) {
                is java.sql.Timestamp -> value.toLocalDateTime()
                is LocalDate -> value.atStartOfDay()
                is LocalDateTime -> value
                is OffsetDateTime -> value.toLocalDateTime()
                is java.util.Date -> LocalDateTime.ofInstant(value.toInstant(), java.time.ZoneOffset.UTC)
                else -> runCatching { LocalDateTime.parse(value.toString()) }.getOrNull()
            }
            Schema.TypeName.DECIMAL -> when (value) {
                is java.math.BigDecimal -> value
                is Number -> java.math.BigDecimal.valueOf(value.toDouble())
                else -> runCatching { java.math.BigDecimal(value.toString()) }.getOrNull()
            }
            Schema.TypeName.BYTES -> when (value) {
                is ByteArray -> value
                is String -> value.toByteArray(Charsets.UTF_8)
                else -> value.toString().toByteArray(Charsets.UTF_8)
            }
            else -> value.toString()
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(ClickHouseReadFn::class.java)
    }
}

/** SQL assembly kept separate so a configured cap remains correct for a query that already has LIMIT. */
internal object ClickHouseReadSql {
    fun withMaxRows(query: String, maxRows: Int): String =
        if (maxRows == 0) query else "SELECT * FROM (${query.trimEnd(';')}) AS _hdata_limited_read LIMIT $maxRows"
}
