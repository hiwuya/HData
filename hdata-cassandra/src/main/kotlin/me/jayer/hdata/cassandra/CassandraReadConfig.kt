package me.jayer.hdata.cassandra

import java.io.Serializable

/**
 * Config for `ReadFromCassandra`.
 *
 * ```yaml
 * - type: ReadFromCassandra
 *   config:
 *     endpoints: ["localhost:9042"]
 *     keyspace: "my_keyspace"
 *     query: "SELECT * FROM events"
 *     consistency_level: LOCAL_ONE
 * ```
 *
 * The read side executes [query] against the Cassandra cluster and streams the result rows. The
 * output schema is derived from the result set metadata. The query **must** be a SELECT.
 *
 * @author wuya
 */
data class CassandraReadConfig(
    /** Contact points in `host:port` format; at least one required. */
    val endpoints: List<String> = listOf("localhost:9042"),
    val keyspace: String = "",
    /** CQL SELECT query to execute. */
    val query: String = "",
    /** Consistency level: LOCAL_ONE, LOCAL_QUORUM, ONE, QUORUM, ALL, etc. */
    val consistencyLevel: String = "LOCAL_ONE",
    /** Cassandra datacenter name; default "datacenter1" for single-node / testcontainer. */
    val datacenter: String = "datacenter1",
    /** Maximum number of rows to read; 0 = no limit. */
    val maxRows: Int = 0,
    /** Fetch size for paged results. */
    val fetchSize: Int = 5000,
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
        require(query.isNotBlank()) { "query must not be blank" }
        require(maxRows >= 0) { "max_rows must be >= 0" }
        require(fetchSize > 0) { "fetch_size must be > 0" }
        require(connectTimeoutMs > 0) { "connect_timeout_ms must be > 0" }
        require(requestTimeoutMs > 0) { "request_timeout_ms must > 0" }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
