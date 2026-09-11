package me.jayer.hdata.jdbc.internal

import org.apache.beam.sdk.schemas.Schema
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.ResultSet

/**
 * Metadata probing at graph construction time: look up the schema, the primary key, and the value range of the partition column.
 *
 * @author wuya
 * @date 2022-08-04
 */
object JdbcMetadata {

    private val LOGGER = LoggerFactory.getLogger(JdbcMetadata::class.java)

    /**
     * Does not execute the query; only takes the result set metadata.
     *
     * A few drivers return null before execution, in which case we fall back to executing once with `maxRows` set to 1.
     */
    fun describe(connection: Connection, sql: String): List<JdbcColumn> {
        connection.prepareStatement(sql).use { ps ->
            ps.metaData?.let { metaData ->
                return (1..metaData.columnCount).map { JdbcColumn.from(metaData, it) }
            }
            LOGGER.debug("driver returned no prepared metadata, falling back to a single execution: {}", sql)
            ps.maxRows = 1
            ps.executeQuery().use { rs ->
                val metaData = rs.metaData
                return (1..metaData.columnCount).map { JdbcColumn.from(metaData, it) }
            }
        }
    }

    /**
     * Translates JDBC column metadata into a Beam schema and resolves how each column is read.
     *
     * Duplicate column names (joining two tables that share a column name, for example) make Beam throw a rather cryptic error, so we catch it here and suggest an alias.
     */
    fun toSchema(columns: List<JdbcColumn>): Pair<Schema, List<ResultSetReader>> {
        require(columns.isNotEmpty()) { "the query returned no columns" }

        val duplicated = columns.groupingBy { it.label }.eachCount().filterValues { it > 1 }.keys
        require(duplicated.isEmpty()) {
            "the query result contains duplicate column names $duplicated; use aliases in SQL to tell them apart (for example SELECT a.id AS a_id, b.id AS b_id)"
        }

        val codecs = columns.map { column ->
            requireNotNull(TypeMappings.resolve(column)) {
                "the type of column ${column.describe()} is not supported yet; cast it to a string in SQL before syncing"
            }
        }
        val schema = Schema.builder()
            .addFields(columns.zip(codecs) { column, codec ->
                Schema.Field.of(column.label, codec.fieldType).withNullable(column.nullable)
            })
            .build()
        return schema to codecs.map { it.reader }
    }

    fun describeTable(connection: Connection, table: String): List<JdbcColumn> =
        describe(connection, SelectSql(table).render())

    /**
     * Returns the primary key columns ordered by `KEY_SEQ`.
     *
     * Two pitfalls:
     * 1. The rows returned by `getPrimaryKeys` are **not guaranteed to be ordered** — H2 hands out the KEY_SEQ=2 column first.
     *    Before the refactor we took `firstOrNull()` directly, which picked the wrong column for composite primary keys.
     * 2. The table name case must match what the dictionary stores: H2 / Oracle store uppercase, PostgreSQL stores lowercase.
     *    Passing the wrong one yields an empty result, so auto partitioning **silently does not take effect**: the job degrades to single-threaded reads with no warning.
     */
    fun primaryKeyColumns(connection: Connection, table: String): List<String> {
        val (schema, name) = TableNames.split(table)
        for (candidate in nameVariants(name)) {
            val keys = readPrimaryKeys(connection, schema, candidate)
            if (keys.isNotEmpty()) return keys
        }
        return emptyList()
    }

    private fun nameVariants(name: String): List<String> =
        listOf(name, name.uppercase(), name.lowercase()).distinct()

    private fun readPrimaryKeys(connection: Connection, schema: String?, table: String): List<String> =
        runCatching {
            connection.metaData.getPrimaryKeys(connection.catalog, schema, table).use { rs ->
                buildList {
                    while (rs.next()) {
                        add(rs.getShort("KEY_SEQ") to rs.getString("COLUMN_NAME"))
                    }
                }
            }.sortedBy { it.first }.map { it.second }
        }.getOrElse {
            LOGGER.debug("failed to read the primary key of table [{}]: {}", table, it.message)
            emptyList()
        }

/**
 * Probing result for the partition column: value range plus whether NULL exists.
 *
 * @author wuya
 */
data class PartitionProbe(
    /** Minimum value of the partition column; null for an empty table. */
    val min: Any?,
    /** Maximum value of the partition column; null for an empty table. */
    val max: Any?,
    /** Whether the partition column contains NULL values. */
    val hasNulls: Boolean,
)

/**
 * Metadata probing for the partition column: MIN / MAX / whether NULL exists, all in **one SQL statement, one scan**.
 *
 * NULL detection uses the difference between `COUNT(*)` and `COUNT(col)` — they sit in the same statement as MIN/MAX,
 * replacing the previous two queries ("one min/max pass + one `count(*) WHERE col IS NULL` pass"):
 * COUNT is a genuine full aggregation, so saving one pass is worth it.
 */
fun partitionProbe(connection: Connection, select: SelectSql, column: String): PartitionProbe {
    val sql = select.withColumns("min($column)", "max($column)", "count(*)", "count($column)").render()
    connection.prepareStatement(sql).use { ps ->
        ps.executeQuery().use { rs ->
            if (!rs.next()) return PartitionProbe(null, null, false)
            val min = rs.getObject(1)
            val max = rs.getObject(2)
            val total = rs.getLong(3)
            val nonNull = rs.getLong(4)
            return PartitionProbe(min, max, total > nonNull)
        }
    }
}

    /** Generic single-value query. */
    fun <T> queryOne(connection: Connection, sql: String, extract: (ResultSet) -> T): T? {
        connection.prepareStatement(sql).use { ps ->
            ps.executeQuery().use { rs ->
                return if (rs.next()) extract(rs) else null
            }
        }
    }
}
