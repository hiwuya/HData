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
) : Serializable {
    companion object {
        private const val serialVersionUID: Long = 1
    }

    fun validate() {
        require(table.isNotBlank()) { "table is required not blank" }
        require(batchSize > 0) { "batchSize is required > 0" }
    }
}