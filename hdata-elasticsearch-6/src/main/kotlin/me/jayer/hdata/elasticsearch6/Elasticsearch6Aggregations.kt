package me.jayer.hdata.elasticsearch6

import java.io.Serializable
import org.apache.beam.sdk.schemas.Schema

/**
 * The expression for ES 6.x push-down aggregation: `count` / `min:col` / `max:col` / `sum:col` / `avg:col`.
 * Translated into ES native aggregations ([org.elasticsearch.search.aggregations.AggregationBuilders]), computed on the
 * ES side and returned as a single row.
 *
 * Numeric aggregations (min/max/sum/avg) are uniformly returned as DOUBLE, and count as INT64.
 *
 * @author wuya
 */
data class Es6AggSpec(val op: String, val column: String?) : Serializable {
    init {
        require(op in setOf("count", "min", "max", "sum", "avg")) {
            "ES aggregation only supports count/min/max/sum/avg, not '$op'"
        }
        require(op == "count" || column != null) { "$op requires a column to be specified" }
    }
}

fun parseEs6Aggregations(specs: List<String>): List<Es6AggSpec> = specs.map { raw ->
    require(raw.isNotBlank()) { "aggregations must not contain an empty declaration" }
    val (op, rawColumn) = raw.split(":", limit = 2)
        .let { it[0].trim().lowercase() to it.getOrNull(1)?.trim()?.takeIf(String::isNotEmpty) }
    val column = if (op == "count") {
        require(rawColumn == null || rawColumn == "*") { "count only supports count or count:*, not count:$rawColumn" }
        null
    } else {
        rawColumn
    }
    Es6AggSpec(op, column)
}.also { parsed ->
    val names = parsed.map(::aggregateFieldName)
    require(names.distinct().size == names.size) { "aggregations has duplicate output column names: ${names.joinToString()}" }
}

/** The field name of the aggregation result row, consistent with JDBC/Iceberg/ES8: count / min_col / max_col / sum_col / avg_col. */
fun aggregateFieldName(spec: Es6AggSpec): String = when (spec.op) {
    "count" -> "count"
    "min" -> "min_${spec.column}"
    "max" -> "max_${spec.column}"
    "sum" -> "sum_${spec.column}"
    "avg" -> "avg_${spec.column}"
    else -> error("Unsupported aggregation: ${spec.op}")
}

/** Schema of the aggregation result row: count → INT64, other numeric aggregations → DOUBLE (all nullable). */
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
