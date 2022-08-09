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
    val partitionLowerBound: Long? = null,
    val partitionUpperBound: Long? = null,
    val query: String = "",
    val fetchSize: Int = 10000,
) : Serializable {
    companion object {
        private const val serialVersionUID: Long = 1
    }
}