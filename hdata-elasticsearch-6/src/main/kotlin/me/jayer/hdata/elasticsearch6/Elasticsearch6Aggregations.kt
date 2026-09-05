package me.jayer.hdata.elasticsearch6

import java.io.Serializable
import org.apache.beam.sdk.schemas.Schema

/**
 * ES 6.x 聚合下推的表达式：`count` / `min:col` / `max:col` / `sum:col` / `avg:col`。
 * 翻译成 ES 原生 aggregation（[org.elasticsearch.search.aggregations.AggregationBuilders]），
 * 在 ES 侧算完返回单行。
 *
 * 数值聚合（min/max/sum/avg）统一按 DOUBLE 返回，count 为 INT64。
 *
 * @author wuya
 */
data class Es6AggSpec(val op: String, val column: String?) : Serializable {
    init {
        require(op in setOf("count", "min", "max", "sum", "avg")) {
            "ES 聚合只支持 count/min/max/sum/avg，不支持 '$op'"
        }
        require(op == "count" || column != null) { "$op 需要指定列" }
    }
}

fun parseEs6Aggregations(specs: List<String>): List<Es6AggSpec> = specs.map { raw ->
    require(raw.isNotBlank()) { "aggregations 不能包含空声明" }
    val (op, rawColumn) = raw.split(":", limit = 2)
        .let { it[0].trim().lowercase() to it.getOrNull(1)?.trim()?.takeIf(String::isNotEmpty) }
    val column = if (op == "count") {
        require(rawColumn == null || rawColumn == "*") { "count 只支持 count 或 count:*，不支持 count:$rawColumn" }
        null
    } else {
        rawColumn
    }
    Es6AggSpec(op, column)
}.also { parsed ->
    val names = parsed.map(::aggregateFieldName)
    require(names.distinct().size == names.size) { "aggregations 输出列名重复: ${names.joinToString()}" }
}

/** 聚合结果行的字段名，与 JDBC/Iceberg/ES8 保持一致：count / min_col / max_col / sum_col / avg_col。 */
fun aggregateFieldName(spec: Es6AggSpec): String = when (spec.op) {
    "count" -> "count"
    "min" -> "min_${spec.column}"
    "max" -> "max_${spec.column}"
    "sum" -> "sum_${spec.column}"
    "avg" -> "avg_${spec.column}"
    else -> error("不支持的聚合: ${spec.op}")
}

/** 聚合结果行的 schema：count → INT64，其余数值聚合 → DOUBLE（均可空）。 */
fun buildAggregateSchema(specs: List<Es6AggSpec>): Schema {
    val builder = Schema.builder()
    specs.forEach { spec ->
        when (spec.op) {
            "count" -> builder.addNullableField("count", Schema.FieldType.INT64)
            else -> builder.addNullableField(aggregateFieldName(spec), Schema.FieldType.DOUBLE)
        }
    }
    return builder.build()
}
