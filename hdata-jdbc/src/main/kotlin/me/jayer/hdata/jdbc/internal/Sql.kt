package me.jayer.hdata.jdbc.internal

import java.io.Serializable

/**
 * SELECT 语句的可序列化描述——它会作为 PCollection 的元素在 DAG 里流动，所以是数据而不是 builder。
 *
 * @author wuya
 * @date 2022-08-25
 */
data class SelectSql(
    val table: String,
    val columns: List<String> = listOf("*"),
    val conditions: List<String> = emptyList(),
) : Serializable {

    fun withColumns(vararg columns: String): SelectSql = copy(columns = columns.toList())

    fun withConditions(vararg conditions: String): SelectSql =
        copy(conditions = this.conditions + conditions)

    fun render(): String {
        val selected = columns.filter { it.isNotBlank() }
        require(selected.isNotEmpty()) { "SELECT 至少要有一列" }
        val effective = conditions.filter { it.isNotBlank() }
        val where = if (effective.isEmpty()) "" else " WHERE ${effective.joinToString(" AND ") { "($it)" }}"
        return "SELECT ${selected.joinToString(", ")} FROM $table$where"
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * INSERT 语句。
 */
object InsertSql {

    fun render(table: String, columns: List<String>): String {
        val effective = columns.filter { it.isNotBlank() }
        require(effective.isNotEmpty()) { "INSERT 至少要有一列" }
        return "INSERT INTO $table ${effective.joinToString(", ", "(", ")")} " +
            "VALUES ${effective.joinToString(", ", "(", ")") { "?" }}"
    }
}
