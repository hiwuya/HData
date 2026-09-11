package me.jayer.hdata.cassandra

import java.io.Serializable

/**
 * Config for `WriteToCassandra`.
 *
 * ```yaml
 * - type: WriteToCassandra
 *   config:
 *     endpoints: ["localhost:9042"]
 *     keyspace: "my_keyspace"
 *     table: "events"
 *     batch_size: 50
 *     consistency_level: LOCAL_QUORUM
 * ```
 *
 * Each input row is written via a CQL INSERT. The columns are derived from the input row's schema.
 * Failed rows (after retries) are routed to the dead-letter stream.
 *
 * Cassandra's batch log ensures atomicity for UNLOGGED batches on the same partition. For rows
 * spanning multiple partitions, use LOGGED batches (the default). The batch size should be kept
 * modest to avoid exceeding the Cassandra batch size limit (default 5 KB in older versions,
 * 1 MB in 4.x+).
 *
 * @author wuya
 */
data class CassandraWriteConfig(
    /** Contact points in `host:port` format; at least one required. */
    val endpoints: List<String> = listOf("localhost:9042"),
    val keyspace: String = "",
    /** The target table name. */
    val table: String = "",
    /** Consistency level for writes. */
    val consistencyLevel: String = "LOCAL_ONE",
    /** Flush and send after this many rows accumulate. */
    val batchSize: Int = 50,
    /** Number of retries on transient write failures. 0 = no retries. */
    val maxRetries: Int = 3,
    /** Delay in milliseconds between retries (multiplied by attempt number). */
    val retryDelayMs: Long = 1000,
    /** Use UNLOGGED batch (faster, but not atomic across partitions). */
    val unloggedBatch: Boolean = false,
    /** Connection timeout in milliseconds. */
    val connectTimeoutMs: Int = 10_000,
    /** Request timeout in milliseconds. */
    val requestTimeoutMs: Int = 30_000,
) : Serializable {

    fun validate() {
        require(endpoints.isNotEmpty()) { "endpoints must not be empty" }
        require(endpoints.all { it.isNotBlank() }) { "endpoints must not contain blank entries" }
        require(endpoints.all {
            val parts = it.split(":")
            parts.size == 2 && parts[0].isNotBlank() && parts[1].toIntOrNull() in 1..65535
        }) { "endpoints must be in host:port format with a valid port" }
        require(keyspace.isNotBlank()) { "keyspace must not be blank" }
        require(table.isNotBlank()) { "table must not be blank" }
        require(batchSize > 0) { "batch_size must be > 0" }
        require(maxRetries >= 0) { "max_retries must be >= 0" }
        require(retryDelayMs > 0) { "retry_delay_ms must be > 0" }
        require(connectTimeoutMs > 0) { "connect_timeout_ms must be > 0" }
        require(requestTimeoutMs > 0) { "request_timeout_ms must > 0" }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
