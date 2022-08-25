package io.jayer.hdata.jdbc

import java.io.Serializable
import java.util.*

/**
 * @author wuya
 * @date 2022-07-29
 */
data class JdbcSourceDescriptor(
    val dataSourceConfig: Properties,
    val columns: List<String> = listOf("*"),
    val table: String = "",
    val where: String = "",
    val partitionColumn: String = "",
    val partitionNum: Int? = null,
    val query: String = "",
    val fetchSize: Int = 10000,
) : Serializable {
    companion object {
        private const val serialVersionUID: Long = 1
    }

    fun validate() {
        require(table.isNotBlank() || query.isNotBlank()) { "table or query is required" }
        require(fetchSize > 0) { "fetchSize is required > 0" }
        require(columns.isNotEmpty()) { "columns is required not empty" }
        require(partitionNum == null || partitionNum > 0) { "partitionNum is required > 0" }
    }
}