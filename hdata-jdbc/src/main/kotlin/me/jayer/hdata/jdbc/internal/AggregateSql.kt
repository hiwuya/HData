package me.jayer.hdata.jdbc.internal

import java.io.Serializable

/**
 * Expressions for JDBC push-down aggregation: `count` / `min:col` / `max:col` / `sum:col` / `avg:col`.
 * They are translated directly into native DB aggregate SQL ([renderAggregateSelect]), so the computation really happens
 * on the source side instead of pulling the data into Beam and aggregating there.
 *
 * @author wuya
 */
data class JdbcAggSpec(val op: String, val column: String?) : Serializable {
    init {
        require(op in setOf("count", "min", "max", "sum", "avg")) {
            "JDBC aggregation supports only count/min/max/sum/avg, not '$op'"
        }
        require(op == "count" || column != null) { "$op requires a column" }
    }
}

fun parseJdbcAggregations(specs: List<String>): List<JdbcAggSpec> = specs.map { raw ->
    val (op, col) = raw.split(":", limit = 2)
        .let { it[0].lowercase() to it.getOrNull(1)?.takeIf { c -> c.isNotBlank() } }
    JdbcAggSpec(op, col)
}

/** Output column name of each aggregation: count / min_<col> / max_<col> / sum_<col> / avg_<col> (shared by validation and rendering so they stay consistent). */
fun jdbcAggOutputName(spec: JdbcAggSpec): String =
    if (spec.op == "count") "count" else "${spec.op}_${spec.column}"

/** Each aggregation is translated into a SELECT expression with an alias, and the alias is double-quoted to pin its case
 *  (count / min_col / ...), keeping it consistent with the Iceberg aggregation output; double quotes are the ANSI identifier quote (H2/Postgres support it directly, MySQL needs ANSI_QUOTES enabled). */
fun renderAggregateSelect(specs: List<JdbcAggSpec>): String = specs.joinToString(", ") { spec ->
    val (op, col) = spec
    when (op) {
        "count" -> "COUNT(${col ?: "*"}) AS \"${jdbcAggOutputName(spec)}\""
        "min" -> "MIN($col) AS \"${jdbcAggOutputName(spec)}\""
        "max" -> "MAX($col) AS \"${jdbcAggOutputName(spec)}\""
        "sum" -> "SUM($col) AS \"${jdbcAggOutputName(spec)}\""
        "avg" -> "AVG($col) AS \"${jdbcAggOutputName(spec)}\""
        else -> error("unsupported aggregation: $op")
    }
}
