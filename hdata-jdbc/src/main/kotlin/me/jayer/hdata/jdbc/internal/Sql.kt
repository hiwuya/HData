package me.jayer.hdata.jdbc.internal

import java.io.Serializable

/**
 * A serializable description of a SELECT statement — it flows through the DAG as a PCollection element, so it is data, not a builder.
 *
 * @author wuya
 * @date 2022-08-25
 */
data class SelectSql(
    val table: String,
    val columns: List<String> = listOf("*"),
    val conditions: List<String> = emptyList(),
    /** Maximum number of rows to read; `<= 0` means unlimited. Pushed down as the SQL `LIMIT`. */
    val limit: Long = -1,
) : Serializable {

    fun withColumns(vararg columns: String): SelectSql = copy(columns = columns.toList())

    fun withConditions(vararg conditions: String): SelectSql =
        copy(conditions = this.conditions + conditions)

    fun render(): String {
        val selected = columns.filter { it.isNotBlank() }
        require(selected.isNotEmpty()) { "SELECT must have at least one column" }
        val effective = conditions.filter { it.isNotBlank() }
        val where = if (effective.isEmpty()) "" else " WHERE ${effective.joinToString(" AND ") { "($it)" }}"
        val limitClause = if (limit > 0) " LIMIT $limit" else ""
        return "SELECT ${selected.joinToString(", ")} FROM $table$where$limitClause"
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * INSERT statement.
 */
object InsertSql {

    fun render(table: String, columns: List<String>): String {
        val effective = columns.filter { it.isNotBlank() }
        require(effective.isNotEmpty()) { "INSERT must have at least one column" }
        return "INSERT INTO $table ${effective.joinToString(", ", "(", ")")} " +
            "VALUES ${effective.joinToString(", ", "(", ")") { "?" }}"
    }
}
