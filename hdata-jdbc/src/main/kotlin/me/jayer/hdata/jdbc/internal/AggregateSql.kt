package me.jayer.hdata.jdbc.internal

import java.io.Serializable

/**
 * JDBC 聚合下推的表达式：`count` / `min:col` / `max:col` / `sum:col` / `avg:col`。
 * 直接翻译成 DB 原生聚合 SQL（[renderAggregateSelect]），真正做到在数据源侧算完，
 * 而不是把数据拉到 Beam 上再聚。
 *
 * @author wuya
 */
data class JdbcAggSpec(val op: String, val column: String?) : Serializable {
    init {
        require(op in setOf("count", "min", "max", "sum", "avg")) {
            "JDBC 聚合只支持 count/min/max/sum/avg，不支持 '$op'"
        }
        require(op == "count" || column != null) { "$op 需要指定列" }
    }
}

fun parseJdbcAggregations(specs: List<String>): List<JdbcAggSpec> = specs.map { raw ->
    val (op, col) = raw.split(":", limit = 2)
        .let { it[0].lowercase() to it.getOrNull(1)?.takeIf { c -> c.isNotBlank() } }
    JdbcAggSpec(op, col)
}

/** 每个聚合翻译成带别名的 SELECT 表达式，别名用双引号包住以锁定大小写（count / min_col / ...），
 *  与 Iceberg 聚合输出保持一致；双引号是 ANSI 标识符引号（H2/Postgres 直接支持，MySQL 需开启 ANSI_QUOTES）。 */
fun renderAggregateSelect(specs: List<JdbcAggSpec>): String = specs.joinToString(", ") { (op, col) ->
    when (op) {
        "count" -> "COUNT(${col ?: "*"}) AS \"count\""
        "min" -> "MIN($col) AS \"min_$col\""
        "max" -> "MAX($col) AS \"max_$col\""
        "sum" -> "SUM($col) AS \"sum_$col\""
        "avg" -> "AVG($col) AS \"avg_$col\""
        else -> error("不支持的聚合: $op")
    }
}
