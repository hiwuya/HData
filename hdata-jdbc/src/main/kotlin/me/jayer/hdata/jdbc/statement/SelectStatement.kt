package me.jayer.hdata.jdbc.statement

/**
 * @author wuya
 * @date 2022-08-25
 */
class SelectStatement(
    val columns: List<String> = emptyList(),
    val table: String = "",
    val where: List<String> = emptyList(),
) : me.jayer.hdata.jdbc.statement.Statement {

    fun copy(
        columns: List<String> = this.columns,
        table: String = this.table,
        where: List<String> = this.where
    ): me.jayer.hdata.jdbc.statement.SelectStatement {
        return me.jayer.hdata.jdbc.statement.SelectStatement(columns, table, where)
    }

    fun columns(vararg columns: String): me.jayer.hdata.jdbc.statement.SelectStatement {
        return columns(columns.toList())
    }

    fun columns(columns: List<String>): me.jayer.hdata.jdbc.statement.SelectStatement {
        return copy(columns = columns)
    }

    fun appendWhere(vararg condition: String): me.jayer.hdata.jdbc.statement.SelectStatement {
        return appendWhere(condition.toList())
    }

    fun appendWhere(conditions: List<String>): me.jayer.hdata.jdbc.statement.SelectStatement {
        return copy(where = where.toMutableList().apply { addAll(conditions) })
    }

    override fun buildSql(): String {
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