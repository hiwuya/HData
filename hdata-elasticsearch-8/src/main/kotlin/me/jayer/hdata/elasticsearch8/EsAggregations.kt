package me.jayer.hdata.elasticsearch8

import java.io.Serializable
import org.apache.beam.sdk.schemas.Schema

/**
 * Aggregate pushdown expressions: `count`, `min:col`, `max:col`, `sum:col`, and `avg:col`.
 * They become native Elasticsearch aggregations and return one row without loading documents into Beam.
 *
 * Numeric aggregations return DOUBLE because Elasticsearch returns doubles; count returns INT64.
 *
 * @author wuya
 */
data class EsAggSpec(val op: String, val column: String?) : Serializable {
    init {
        require(op in setOf("count", "min", "max", "sum", "avg")) {
            "Elasticsearch aggregations support only count/min/max/sum/avg, not '$op'"
        }
        require(op == "count" || column != null) { "$op requires a column" }
    }
}

fun parseEsAggregations(specs: List<String>): List<EsAggSpec> = specs.map { raw ->
    require(raw.isNotBlank()) { "aggregations must not contain an empty declaration" }
    val (op, rawColumn) = raw.split(":", limit = 2)
        .let { it[0].trim().lowercase() to it.getOrNull(1)?.trim()?.takeIf(String::isNotEmpty) }
    val column = if (op == "count") {
        require(rawColumn == null || rawColumn == "*") { "count supports only count or count:*, not count:$rawColumn" }
        null
    } else {
        rawColumn
    }
    EsAggSpec(op, column)
}.also { parsed ->
    val names = parsed.map(::aggregateFieldName)
    require(names.distinct().size == names.size) { "aggregations produce duplicate output fields: ${names.joinToString()}" }
}

/** Aggregate result field names, aligned with JDBC and Iceberg: count, min_col, max_col, sum_col, avg_col. */
fun aggregateFieldName(spec: EsAggSpec): String = when (spec.op) {
    "count" -> "count"
    "min" -> "min_${spec.column}"
    "max" -> "max_${spec.column}"
    "sum" -> "sum_${spec.column}"
    "avg" -> "avg_${spec.column}"
    else -> error("Unsupported aggregation: ${spec.op}")
}

/** Aggregate result schema: count is INT64; other nullable numeric values are DOUBLE. */
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
