package me.jayer.hdata.jdbc.statement

/**
 * @author wuya
 * @date 2022-08-25
 */
class InsertStatement(
    val columns: List<String> = emptyList(),
    val table: String = ""
) : me.jayer.hdata.jdbc.statement.Statement {

    fun copy(
        columns: List<String> = this.columns,
        table: String = this.table,
    ): me.jayer.hdata.jdbc.statement.InsertStatement {
        return me.jayer.hdata.jdbc.statement.InsertStatement(columns, table)
    }

    fun columns(vararg columns: String): me.jayer.hdata.jdbc.statement.InsertStatement {
        return columns(columns.toList())
    }

    fun columns(columns: List<String>): me.jayer.hdata.jdbc.statement.InsertStatement {
        return copy(columns = columns)
    }

    override fun buildSql(): String {
        return "INSERT INTO $table ${
            columns.filter { it.isNotBlank() }.joinToString(", ", "(", ")")
        } VALUES ${columns.filter { it.isNotBlank() }.joinToString(", ", "(", ")") { "?" }}"
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}