package me.jayer.hdata.jdbc

import java.io.Serializable

/**
 * Config of `WriteToJdbc`.
 *
 * ```yaml
 * - type: WriteToJdbc
 *   config:
 *     url: jdbc:mysql://127.0.0.1:3306/dw
 *     user: root
 *     password: ${MYSQL_PASSWORD}
 *     table: t_order
 *     batch_size: 5000
 *     error_handling:
 *       output: errors
 * ```
 *
 * @author wuya
 * @date 2022-07-29
 */
data class JdbcWriteConfig(
    override val url: String = "",
    override val user: String = "",
    override val password: String = "",
    override val driverClass: String = "",
    override val connectionProperties: Map<String, String> = emptyMap(),
    val table: String = "",
    val batchSize: Int = 10000,
    val retryMaxAttempts: Int = 3,
    val retryInitialSeconds: Long = 3,
    val retryMaxSeconds: Long = 60,
    /** Explicitly accepts duplicate effects when an upstream full-replay source is restarted. */
    val allowDuplicateReplay: Boolean = false,
) : JdbcConnectionConfig, Serializable {

    companion object {
        private const val serialVersionUID: Long = 1
    }

    fun validate() {
        validateConnection()
        require(table.isNotBlank()) { "table must not be blank" }
        require(batchSize > 0) { "batch_size must be > 0" }
        require(retryMaxAttempts > 0) { "retry_max_attempts must be > 0" }
        require(retryInitialSeconds > 0) { "retry_initial_seconds must be > 0" }
        require(retryMaxSeconds >= retryInitialSeconds) { "retry_max_seconds must be >= retry_initial_seconds" }
    }
}
