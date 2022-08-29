package io.jayer.hdata.jdbc

import java.io.Serializable
import java.util.*

/**
 * @author wuya
 * @date 2022-07-29
 */
data class JdbcSinkDescriptor(
    val dataSourceConfig: Properties,
    val table: String = "",
    val batchSize: Int = 10000,
    val retryMaxAttempts: Int = 3,
    val retryInitialSeconds: Long = 3,
    val retryMaxSeconds: Long = 60,
) : Serializable {
    companion object {
        private const val serialVersionUID: Long = 1
    }

    fun validate() {
        require(table.isNotBlank()) { "table is required not blank" }
        require(batchSize > 0) { "batchSize is required > 0" }
        require(retryMaxAttempts > 0) { "retryMaxAttempts is required > 0" }
        require(retryInitialSeconds > 0) { "retryInitialSeconds is required > 0" }
        require(retryMaxSeconds > 0) { "retryMaxSeconds is required > 0" }
    }
}