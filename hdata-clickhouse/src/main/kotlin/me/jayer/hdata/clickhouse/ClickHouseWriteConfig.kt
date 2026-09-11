package me.jayer.hdata.clickhouse

import java.io.Serializable

/**
 * Config for `WriteToClickHouse`.
 *
 * ```yaml
 * - type: WriteToClickHouse
 *   config:
 *     endpoint: "http://localhost:8123"
 *     database: "default"
 *     table: "events"
 *     batch_size: 10000
 * ```
 *
 * Each input row is written to the specified [table]. The input row's schema is used to build the
 * column list for the INSERT statement. Column order follows the row schema's field order.
 *
 * Failed rows (after retries) are routed to the dead-letter stream. The write side is at-least-once:
 * a retried batch may duplicate rows if the first attempt actually succeeded but the client lost
 * the connection before receiving the response. Use `ReplacingMergeTree` or deduplicate downstream
 * for exactly-once semantics.
 *
 * @author wuya
 */
data class ClickHouseWriteConfig(
    /** Server endpoint including protocol, e.g. `http://localhost:8123` or `https://cloud.clickhouse.com`. */
    val endpoint: String = "http://localhost:8123",
    val database: String = "default",
    val username: String = "default",
    val password: String = "",
    /** The target table name. */
    val table: String = "",
    /** Flush and send after this many rows accumulate; also the max in-flight count. */
    val batchSize: Int = 10_000,
    /** Number of retries on transient write failures (network errors, 5xx responses). 0 = no retries. */
    val maxRetries: Int = 3,
    /** Delay in milliseconds between retries (exponential back-off multiplier). */
    val retryDelayMs: Long = 1000,
    /** Connection timeout in milliseconds. */
    val connectTimeoutMs: Int = 10_000,
    /** Socket timeout in milliseconds. */
    val socketTimeoutMs: Int = 60_000,
    /**
     * Optional explicit column names for the INSERT statement. When empty, the input row schema's
     * field names are used in order. Use this when the row field names differ from the table column
     * names.
     */
    val columnNames: List<String> = emptyList(),
) : Serializable {

    fun validate() {
        require(endpoint.isNotBlank()) { "endpoint must not be blank" }
        require(database.isNotBlank()) { "database must not be blank" }
        require(username.isNotBlank()) { "username must not be blank" }
        require(table.isNotBlank()) { "table must not be blank" }
        require(batchSize > 0) { "batch_size must be > 0" }
        require(maxRetries >= 0) { "max_retries must be >= 0" }
        require(retryDelayMs > 0) { "retry_delay_ms must be > 0" }
        require(connectTimeoutMs > 0) { "connect_timeout_ms must be > 0" }
        require(socketTimeoutMs > 0) { "socket_timeout_ms must be > 0" }
    }

    /** Build the INSERT column list from the row schema's field names. */
    fun resolvedColumns(fieldNames: List<String>): List<String> =
        if (columnNames.isNotEmpty()) {
            require(columnNames.size == fieldNames.size) {
                "column_names size (${columnNames.size}) must match input row schema field count (${fieldNames.size})"
            }
            columnNames
        } else {
            fieldNames
        }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
