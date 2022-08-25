package io.jayer.hdata.jdbc.statement

/**
 * @author wuya
 * @date 2022-08-25
 */
class SelectStatement(
    val columns: List<String> = emptyList(),
    val table: String = "",
    val where: List<String> = emptyList(),
) : Statement {

    fun copy(
        columns: List<String> = this.columns,
        table: String = this.table,
        where: List<String> = this.where
    ): SelectStatement {
        return SelectStatement(columns, table, where)
    }

    fun columns(vararg columns: String): SelectStatement {
        return columns(columns.toList())
    }

    fun columns(columns: List<String>): SelectStatement {
        return copy(columns = columns)
    }

    fun appendWhere(vararg condition: String): SelectStatement {
        return appendWhere(condition.toList())
    }

    fun appendWhere(conditions: List<String>): SelectStatement {
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