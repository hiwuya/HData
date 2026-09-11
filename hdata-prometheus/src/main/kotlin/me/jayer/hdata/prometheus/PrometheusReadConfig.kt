package me.jayer.hdata.prometheus

import java.io.Serializable

/**
 * Config for `ReadFromPrometheus`.
 *
 * ```yaml
 * - type: ReadFromPrometheus
 *   config:
 *     endpoint: "http://localhost:9090"
 *     query: "up"
 * ```
 *
 * The read side executes an instant query (`/api/v1/query`) against the Prometheus server and
 * returns one row per time series. The output schema is fixed:
 *  - `metric_name` STRING
 *  - `labels`      MAP<STRING, STRING>
 *  - `value`       DOUBLE
 *  - `timestamp`   DOUBLE  (unix epoch seconds)
 *
 * @author wuya
 */
data class PrometheusReadConfig(
    /** Prometheus server base URL, e.g. `http://localhost:9090`. */
    val endpoint: String = "http://localhost:9090",
    /** PromQL instant query (e.g. `up`, `rate(http_requests_total[5m])`). */
    val query: String = "",
    /** Optional evaluation timestamp; empty = server default (now). */
    val time: String = "",
    /** HTTP connection timeout in milliseconds. */
    val connectTimeoutMs: Int = 10_000,
    /** HTTP read timeout in milliseconds. */
    val readTimeoutMs: Int = 30_000,
) : Serializable {

    fun validate() {
        require(endpoint.isNotBlank()) { "endpoint must not be blank" }
        require(query.isNotBlank()) { "query must not be blank" }
        require(connectTimeoutMs > 0) { "connect_timeout_ms must be > 0" }
        require(readTimeoutMs > 0) { "read_timeout_ms must be > 0" }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
