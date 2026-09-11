package me.jayer.hdata.jdbc

import java.io.Serializable
import me.jayer.hdata.jdbc.internal.jdbcAggOutputName
import me.jayer.hdata.jdbc.internal.parseJdbcAggregations
import me.jayer.hdata.jdbc.internal.TableNames

/**
 * Config of `ReadFromJdbc`.
 *
 * ```yaml
 * - type: ReadFromJdbc
 *   config:
 *     url: jdbc:mysql://127.0.0.1:3306/demo
 *     user: root
 *     password: ${MYSQL_PASSWORD}
 *     tables: ["t_order_${00-15}"]
 *     where: "created_at >= '2022-01-01'"
 *     partition_column: id
 *     partition_num: 8
 *     limit: 1000
 * ```
 *
 * @author wuya
 * @date 2022-07-29
 */
data class JdbcReadConfig(
    override val url: String = "",
    override val user: String = "",
    override val password: String = "",
    override val driverClass: String = "",
    override val connectionProperties: Map<String, String> = emptyMap(),
    val columns: List<String> = listOf("*"),
    /** Supports range syntax such as `t_order_${00-15}`; see [me.jayer.hdata.jdbc.util.JdbcUtils.resolveTables]. */
    val tables: List<String> = emptyList(),
    val where: String = "",
    val partitionColumn: String = "",
    /** When omitted, it is estimated automatically from the value range of the partition column. */
    val partitionNum: Int? = null,
    /** Give a raw SQL statement; then [tables] / [where] / partitioning all have no effect. */
    val query: String = "",
    /** Maximum number of rows to read; `-1` means unlimited. Pushed down as the SQL `LIMIT` (only in [tables] mode, ignored in [query] mode). */
    val limit: Long = -1,
    val fetchSize: Int = 10000,
    /**
     * Push-down aggregation: `["count", "min:age", "max:age", "sum:age", "avg:age"]`. Translated into native DB aggregate SQL,
     * evaluated on the source side and returned as a single row. When non-empty, columns / partitioning / limit are ignored; only a single table or query is supported.
     */
    val aggregations: List<String> = emptyList(),
) : JdbcConnectionConfig, Serializable {

    companion object {
        private const val serialVersionUID: Long = 1
        private const val MAX_PARTITION_NUM: Int = 10_000
    }

    fun validate() {
        validateConnection()
        require(tables.isNotEmpty() || query.isNotBlank()) { "at least one of tables and query must be set" }
        require(tables.isEmpty() || query.isBlank()) { "tables and query must not both be set" }
        require(fetchSize > 0) { "fetch_size must be > 0" }
        require(columns.isNotEmpty()) { "columns must not be empty" }
        require(columns.none { it.isBlank() }) { "columns must not contain a blank column name" }
        require(tables.none { it.isBlank() }) { "tables must not contain a blank table name" }
        require(columns.distinct().size == columns.size) { "columns must not contain duplicates" }
        val resolvedTables = TableNames.resolve(tables)
        require(resolvedTables.distinct().size == resolvedTables.size) {
            "tables must not contain duplicates after expansion, otherwise the same table would be read more than once"
        }
        require(partitionNum == null || partitionNum > 0) { "partition_num must be > 0" }
        require(partitionNum == null || partitionNum <= MAX_PARTITION_NUM) {
            "partition_num must not exceed $MAX_PARTITION_NUM; too many concurrent connections will overwhelm the source database"
        }
        require(limit == -1L || limit > 0) { "limit must be > 0 (or leave it empty / pass -1 for unlimited)" }
        require(limit <= 0 || resolvedTables.size <= 1) {
            "limit is a global row cap; reading multiple tables at once is not supported yet, otherwise it would degrade into taking $limit rows from each table"
        }
        if (limit > 0) {
            require(partitionColumn.isBlank() && (partitionNum == null || partitionNum == 1)) {
                "limit mode forces a single query and does not use partition_column, so partition_num must be left empty or set to 1"
            }
        }
        if (aggregations.isNotEmpty()) {
            // Aggregation is computed on the DB side and returns a single row, so columns / partitioning / limit are meaningless
            require(columns == listOf("*")) { "aggregations mode does not use columns, please remove it from the config" }
            require(partitionColumn.isBlank()) { "aggregations mode does not use partition_column, please remove it from the config" }
            require(partitionNum == null) { "aggregations mode does not use partition_num, please remove it from the config" }
            require(limit == -1L) { "aggregations mode does not use limit, please remove it from the config" }
            require(resolvedTables.size <= 1) { "aggregations supports only a single table or query (after table range expansion there must be exactly one table)" }
            val parsed = parseJdbcAggregations(aggregations) // Rejects unsupported aggregations (sum/avg are allowed)
            // Duplicate output column names put two identically named columns into the result set, which corrupts the schema outright; report it up front
            val names = parsed.map(::jdbcAggOutputName)
            require(names.distinct().size == names.size) { "aggregations output column names are duplicated: $names" }
        }
        if (query.isNotBlank()) {
            require(columns == listOf("*")) { "query mode does not use columns, please remove it from the config" }
            require(where.isBlank()) { "query mode does not use where, please remove it from the config" }
            require(partitionColumn.isBlank()) { "query mode does not use partition_column, please remove it from the config" }
            require(partitionNum == null) { "query mode does not use partition_num, please remove it from the config" }
            require(limit == -1L) { "query mode does not use limit, please remove it from the config" }
        }
    }
}
