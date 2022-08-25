package io.jayer.hdata.jdbc.statement

import java.io.Serializable

/**
 * @author wuya
 * @date 2022-08-25
 */
data class SelectStatement(
    val columns: List<String> = emptyList(),
    val table: String = "",
    val where: List<String> = emptyList(),
) : Serializable {

    fun columns(vararg columns: String): SelectStatement {
        return columns(columns.toList())
    }

    fun columns(columns: List<String>): SelectStatement {
        return this.copy(columns = columns)
    }

    fun appendWhere(vararg condition: String): SelectStatement {
        return appendWhere(condition.toList())
    }

    fun appendWhere(conditions: List<String>): SelectStatement {
        return this.copy(where = this.where.toMutableList().apply { addAll(conditions) })
    }

    fun buildSql(): String {
        var sql = "SELECT ${columns.filter { it.isNotBlank() }.joinToString(", ")} FROM $table"
        val conditions = where.filter { it.isNotBlank() }
        if (conditions.isNotEmpty()) {
            sql += " WHERE ${conditions.joinToString(" AND ") { "($it)" }}"
        }
        return sql
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}