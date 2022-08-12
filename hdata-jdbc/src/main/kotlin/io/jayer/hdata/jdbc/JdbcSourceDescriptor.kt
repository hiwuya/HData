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

    fun createSchemaQuery(): String {
        return query.ifBlank {
            var sql = "SELECT ${columns.joinToString(",")} FROM `$table`"
            if (where.isNotBlank()) {
                sql += " WHERE $where"
            }
            sql
        }
    }
}