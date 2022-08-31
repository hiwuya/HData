package io.jayer.hdata.jdbc

import com.fasterxml.jackson.annotation.JsonProperty
import java.io.Serializable
import java.util.*

/**
 * @author wuya
 * @date 2022-07-29
 */
data class JdbcSourceDescriptor(
    @field:JsonProperty("dataSource")
    val dataSourceConfig: Properties = Properties(),
    val columns: List<String> = listOf("*"),
    val tables: List<String> = emptyList(),
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
        require(tables.isNotEmpty() || query.isNotBlank()) { "tables or query is required" }
        require(fetchSize > 0) { "fetchSize is required > 0" }
        require(columns.isNotEmpty()) { "columns is required not empty" }
        require(partitionNum == null || partitionNum > 0) { "partitionNum is required > 0" }
    }
}