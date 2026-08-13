package me.jayer.hdata.jdbc

import java.io.Serializable

/**
 * `WriteToJdbc` 的配置。
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
) : JdbcConnectionConfig, Serializable {

    companion object {
        private const val serialVersionUID: Long = 1
    }

    fun validate() {
        validateConnection()
        require(table.isNotBlank()) { "table 不能为空" }
        require(batchSize > 0) { "batch_size 必须 > 0" }
        require(retryMaxAttempts > 0) { "retry_max_attempts 必须 > 0" }
        require(retryInitialSeconds > 0) { "retry_initial_seconds 必须 > 0" }
        require(retryMaxSeconds >= retryInitialSeconds) { "retry_max_seconds 必须 >= retry_initial_seconds" }
    }
}
