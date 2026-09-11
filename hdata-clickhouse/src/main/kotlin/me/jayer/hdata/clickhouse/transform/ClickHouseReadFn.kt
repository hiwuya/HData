package me.jayer.hdata.clickhouse.transform

import me.jayer.hdata.clickhouse.ClickHouseReadConfig
import me.jayer.hdata.clickhouse.internal.ClickHouseTypeMappings
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
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
 * This is a plain DoFn (not an SDF): parallelism comes from the number of input elements. The
 * read provider supplies a single trigger element, so the query runs exactly once.
 *
 * @author wuya
 */
class ClickHouseReadFn(
    private val config: ClickHouseReadConfig,
) : DoFn<Any, Row>() {

    @Transient
    private var connection: Connection? = null

    @Setup
    fun setup() {
        val jdbcUrl = buildJdbcUrl()
        connection = DriverManager.getConnection(jdbcUrl, config.username, config.password)
        connection!!.autoCommit = false
    }

    @Teardown
    fun tearDown() {
        runCatching { connection?.close() }
        connection = null
    }

    @ProcessElement
    fun processElement(context: ProcessContext) {
        val conn = checkNotNull(connection) { "ClickHouse connection is not initialized" }
        val sql = if (config.maxRows > 0) {
            "${config.query.trimEnd(';')} LIMIT ${config.maxRows}"
        } else {
            config.query
        }

        LOGGER.info("Executing ClickHouse query: {}", sql)
        val stmt = conn.createStatement()
        try {
            stmt.fetchSize = 10_000
            val rs: ResultSet = stmt.executeQuery(sql)
            try {
                val metaData = rs.metaData
                val columnCount = metaData.columnCount

                // Derive Beam schema from JDBC metadata.
                val schema = deriveSchema(metaData, columnCount)

                // Iterate over result rows.
                while (rs.next()) {
                    val row = convertRow(rs, schema, columnCount)
                    context.output(row)
                }
            } finally {
                rs.close()
            }
        } finally {
            stmt.close()
        }
    }

    private fun deriveSchema(metaData: java.sql.ResultSetMetaData, columnCount: Int): Schema {
        val builder = Schema.builder()
        for (i in 1..columnCount) {
            val columnName = metaData.getColumnLabel(i)
            val typeName = metaData.getColumnTypeName(i)
            val nullable = metaData.isNullable(i) != java.sql.ResultSetMetaData.columnNoNulls

            val beamType = ClickHouseTypeMappings.toBeamType(typeName)
            if (nullable) {
                builder.addNullableField(columnName, beamType)
            } else {
                builder.addField(columnName, beamType)
            }
        }
        return builder.build()
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

    private fun buildJdbcUrl(): String {
        val endpoint = config.endpoint.trimEnd('/')
        val base = if (endpoint.startsWith("http://", ignoreCase = true) ||
            endpoint.startsWith("https://", ignoreCase = true)
        ) {
            // Convert HTTP endpoint to JDBC URL.
            val host = try {
                java.net.URI(endpoint).host ?: "localhost"
            } catch (_: Exception) {
                "localhost"
            }
            val port = try {
                java.net.URI(endpoint).port.takeIf { it > 0 } ?: 8123
            } catch (_: Exception) {
                8123
            }
            "jdbc:clickhouse://$host:$port"
        } else {
            endpoint
        }
        return "$base/${config.database}"
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(ClickHouseReadFn::class.java)
    }
}
