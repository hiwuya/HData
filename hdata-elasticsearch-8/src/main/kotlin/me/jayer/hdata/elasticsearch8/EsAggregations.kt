package me.jayer.hdata.elasticsearch8

import java.io.Serializable
import org.apache.beam.sdk.schemas.Schema

/**
 * ES 聚合下推的表达式：`count` / `min:col` / `max:col` / `sum:col` / `avg:col`。
 * 翻译成 ES 原生 aggregation（[co.elastic.clients.elasticsearch._types.aggregations.Aggregation]），
 * 在 ES 侧算完返回单行，而不是把文档拉到 Beam 上再聚。
 *
 * 数值聚合（min/max/sum/avg）统一按 DOUBLE 返回（ES 聚合结果即为 double），count 为 INT64。
 *
 * @author wuya
 */
data class EsAggSpec(val op: String, val column: String?) : Serializable {
    init {
        require(op in setOf("count", "min", "max", "sum", "avg")) {
            "ES 聚合只支持 count/min/max/sum/avg，不支持 '$op'"
        }
        require(op == "count" || column != null) { "$op 需要指定列" }
    }
}

fun parseEsAggregations(specs: List<String>): List<EsAggSpec> = specs.map { raw ->
    val (op, col) = raw.split(":", limit = 2)
        .let { it[0].lowercase() to it.getOrNull(1)?.takeIf { c -> c.isNotBlank() } }
    EsAggSpec(op, col)
}

/** 聚合结果行的字段名，与 JDBC/Iceberg 保持一致：count / min_col / max_col / sum_col / avg_col。 */
fun aggregateFieldName(spec: EsAggSpec): String = when (spec.op) {
    "count" -> "count"
    "min" -> "min_${spec.column}"
    "max" -> "max_${spec.column}"
    "sum" -> "sum_${spec.column}"
    "avg" -> "avg_${spec.column}"
    else -> error("不支持的聚合: ${spec.op}")
}

/** 聚合结果行的 schema：count → INT64，其余数值聚合 → DOUBLE（均可空）。 */
fun buildAggregateSchema(specs: List<EsAggSpec>): Schema {
    val builder = Schema.builder()
    specs.forEach { spec ->
        when (spec.op) {
            "count" -> builder.addNullableField("count", Schema.FieldType.INT64)
            else -> builder.addNullableField(aggregateFieldName(spec), Schema.FieldType.DOUBLE)
        }
    }
    return builder.build()
}
