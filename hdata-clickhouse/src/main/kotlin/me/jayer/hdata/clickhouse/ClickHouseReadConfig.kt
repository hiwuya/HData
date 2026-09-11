package me.jayer.hdata.clickhouse

import java.io.Serializable

/**
 * Config for `ReadFromClickHouse`.
 *
 * ```yaml
 * - type: ReadFromClickHouse
 *   config:
 *     endpoint: "http://localhost:8123"
 *     database: "default"
 *     query: "SELECT * FROM events"
 * ```
 *
 * The read side executes [query] against the ClickHouse server and streams the result rows. The
 * output schema is derived automatically from the result set metadata. The query **must** return
 * results (i.e. be a SELECT); DDL/DML statements are not supported on the read side.
 *
 * For partition-aware parallel reads, provide multiple `WHERE` conditions in the query or use the
 * ClickHouse `_shard_num` / partition functions to split the work.
 *
 * @author wuya
 */
data class ClickHouseReadConfig(
    /** Server endpoint including protocol, e.g. `http://localhost:8123` or `https://cloud.clickhouse.com`. */
    val endpoint: String = "http://localhost:8123",
    val database: String = "default",
    val username: String = "default",
    val password: String = "",
    /** The SQL query to execute; must be a SELECT. */
    val query: String = "",
    /** Maximum number of rows to read; 0 = no limit. */
    val maxRows: Int = 0,
    /** Connection timeout in milliseconds. */
    val connectTimeoutMs: Int = 10_000,
    /** Socket timeout in milliseconds. */
    val socketTimeoutMs: Int = 60_000,
) : Serializable {

    fun validate() {
        require(endpoint.isNotBlank()) { "endpoint must not be blank" }
        require(database.isNotBlank()) { "database must not be blank" }
        require(username.isNotBlank()) { "username must not be blank" }
        require(query.isNotBlank()) { "query must not be blank" }
        require(maxRows >= 0) { "max_rows must be >= 0" }
        require(connectTimeoutMs > 0) { "connect_timeout_ms must be > 0" }
        require(socketTimeoutMs > 0) { "socket_timeout_ms must be > 0" }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
