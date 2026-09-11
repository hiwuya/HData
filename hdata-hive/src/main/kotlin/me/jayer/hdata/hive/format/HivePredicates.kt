package me.jayer.hdata.hive.format

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import java.io.Serializable
import java.math.BigDecimal
import java.nio.charset.StandardCharsets

/**
 * Read-side predicate pushdown, mirroring the Trino Hive connector's `TupleDomain`:
 * the column statistics (min/max) of ORC stripes / Parquet row groups are used to skip units that cannot match the predicate,
 * with a row-level filter as a safety net to keep the result correct.
 *
 * Like Trino, only simple comparison predicates (ANDed) on **data columns** are pushed down; pruning of partition columns still
 * goes through `partition_filter`. Pushdown can skip at the file statistics level for numeric (byte/short/int/long/float/double),
 * decimal and string columns — these three cover the vast majority of `WHERE` filters; date/timestamp/boolean/nested types only
 * get row-level filtering, so correctness is unaffected.
 */

enum class PredicateOp {
    EQ, NEQ, GT, GTE, LT, LTE, IS_NULL, IS_NOT_NULL
}

/** Unified comparable representation of predicate values and column statistics min/max values. */
sealed interface ValueRepr : Serializable {
    fun compareTo(other: ValueRepr): Int
}

data class NumericValue(val v: BigDecimal) : ValueRepr {
    override fun compareTo(other: ValueRepr): Int =
        if (other is NumericValue) v.compareTo(other.v) else throw IllegalArgumentException("predicate type mismatch: expected numeric")
}

data class BytesValue(val v: ByteArray) : ValueRepr {
    override fun compareTo(other: ValueRepr): Int =
        if (other is BytesValue) unsignedCompare(v, other.v) else throw IllegalArgumentException("predicate type mismatch: expected string")

    override fun equals(other: Any?): Boolean = other is BytesValue && v.contentEquals(other.v)
    override fun hashCode(): Int = v.contentHashCode()
}

/** One parsed comparison predicate. */
data class HivePredicate(
    val column: String,
    val op: PredicateOp,
    val fieldType: Schema.FieldType,
    val value: ValueRepr?,
) : Serializable

/** Statistics summary of one column in a stripe / row group; omitted when not pushable (min/max are null). */
data class ColumnRangeStats(
    val min: ValueRepr?,
    val max: ValueRepr?,
    val hasNull: Boolean,
    /** Whether the whole column in this unit is NULL; only when this is true can `col IS NOT NULL` skip the whole unit. */
    val allNull: Boolean = false,
) : Serializable

object PredicateEvaluator {

    /** Row-level filter: keeps a row if and only if **all** predicates are TRUE (three-valued logic, unknown means dropped). */
    fun matches(row: Row, predicates: List<HivePredicate>): Boolean = predicates.all { matchesOne(row, it) }

    /**
     * Used for partition pruning: decides whether a **constant value** of a column (for example the value restored from the
     * partition directory) satisfies the predicate. Three-valued logic is the same as at row level — when unsure (unknown) it
     */
    fun matchesConstant(p: HivePredicate, cell: Any?): Boolean = eval(p, cell)

    private fun matchesOne(row: Row, p: HivePredicate): Boolean {
        val idx = row.schema.fieldNames.indexOfFirst { it.equals(p.column, ignoreCase = true) }
        if (idx < 0) return true
        return eval(p, row.getValue(idx))
    }

    private fun eval(p: HivePredicate, cell: Any?): Boolean {
        val isNull = cell == null
        return when (p.op) {
            PredicateOp.IS_NULL -> isNull
            PredicateOp.IS_NOT_NULL -> !isNull
            else -> {
                if (isNull || p.value == null) return false
                val cmp = toRepr(cell, p.fieldType)?.compareTo(p.value)
                    ?: throw IllegalStateException("cannot convert field value [$cell] into a predicate-comparable representation")
                when (p.op) {
                    PredicateOp.EQ -> cmp == 0
                    PredicateOp.NEQ -> cmp != 0
                    PredicateOp.GT -> cmp > 0
                    PredicateOp.GTE -> cmp >= 0
                    PredicateOp.LT -> cmp < 0
                    PredicateOp.LTE -> cmp <= 0
                    else -> false
                }
            }
        }
    }

    /**
     * Statistics-level skipping: if **any** predicate proves the whole unit cannot match, skip that stripe / row group.
     * All decisions are conservative — when unsure, return false (do not skip, read as usual), so pushdown never loses data.
     */
    fun canSkip(predicates: List<HivePredicate>, stats: Map<String, ColumnRangeStats>): Boolean =
        predicates.any { canSkipOne(it, stats[it.column]) }

    private fun canSkipOne(p: HivePredicate, s: ColumnRangeStats?): Boolean {
        if (s == null) return false
        return when (p.op) {
            // No NULL in this unit, so `col IS NULL` cannot match anything -> skippable
            PredicateOp.IS_NULL -> !s.hasNull
            // `col IS NOT NULL` can only be skipped when the whole column is NULL (otherwise non-NULL rows hide inside)
            PredicateOp.IS_NOT_NULL -> s.allNull
            else -> {
                if (s.min == null || s.max == null || p.value == null) return false
                when (p.op) {
                    PredicateOp.EQ -> p.value.compareTo(s.min) < 0 || p.value.compareTo(s.max) > 0
                    // `!= value` cannot match at all only when the whole column has min==max==value
                    PredicateOp.NEQ -> s.min.compareTo(p.value) == 0 && s.max.compareTo(p.value) == 0
                    PredicateOp.GT -> s.max.compareTo(p.value) <= 0
                    PredicateOp.GTE -> s.max.compareTo(p.value) < 0
                    PredicateOp.LT -> s.min.compareTo(p.value) >= 0
                    PredicateOp.LTE -> s.min.compareTo(p.value) > 0
                    else -> false
                }
            }
        }
    }

    /** Converts a cell value in a row into the comparable representation; unsupported types return null (incomparable when filtering). */
    fun toRepr(cell: Any?, fieldType: Schema.FieldType): ValueRepr? {
        if (cell == null) return null
        return when (fieldType.typeName) {
            Schema.TypeName.BYTE, Schema.TypeName.INT16, Schema.TypeName.INT32, Schema.TypeName.INT64,
            Schema.TypeName.FLOAT, Schema.TypeName.DOUBLE, Schema.TypeName.DECIMAL ->
                NumericValue(BigDecimal(cell.toString()))

            Schema.TypeName.STRING ->
                BytesValue((cell as? String ?: cell.toString()).toByteArray(StandardCharsets.UTF_8))

            else -> null
        }
    }
}

/** A raw predicate from the config (deserialized straight from YAML). */
data class ConfigPredicate(
    val column: String = "",
    val op: String = "",
    val value: String = "",
) : Serializable

private fun unsignedCompare(a: ByteArray, b: ByteArray): Int {
    val n = minOf(a.size, b.size)
    for (i in 0 until n) {
        val x = a[i].toInt() and 0xFF
        val y = b[i].toInt() and 0xFF
        if (x != y) return x - y
    }
    return a.size - b.size
}

/** Parses the `op` text from the config into a [PredicateOp]; an illegal operator throws instead of silently degrading. */
fun parsePredicateOp(text: String): PredicateOp {
    val op = text.trim().lowercase()
    return when (op) {
        "=", "==", "eq" -> PredicateOp.EQ
        "!=", "<>", "neq" -> PredicateOp.NEQ
        ">", "gt" -> PredicateOp.GT
        ">=", "gte", "ge" -> PredicateOp.GTE
        "<", "lt" -> PredicateOp.LT
        "<=", "lte", "le" -> PredicateOp.LTE
        "is null", "isnull" -> PredicateOp.IS_NULL
        "is not null", "isnotnull" -> PredicateOp.IS_NOT_NULL
        else -> throw IllegalArgumentException(
            "unsupported predicate operator: '$text' (choose from: = != > >= < <= is null is not null)"
        )
    }
}

/** Parses a raw config predicate into the [HivePredicate] used for pushdown; unsupported column types fail explicitly here. */
fun parsePredicate(config: ConfigPredicate, fieldType: Schema.FieldType): HivePredicate {
    require(config.column.isNotBlank()) { "predicate is missing column" }
    val op = parsePredicateOp(config.op)
    val value = if (op == PredicateOp.IS_NULL || op == PredicateOp.IS_NOT_NULL) {
        null
    } else {
        require(config.value.isNotBlank()) { "the comparison value of predicate [$config] must not be blank" }
        when (fieldType.typeName) {
            Schema.TypeName.BYTE, Schema.TypeName.INT16, Schema.TypeName.INT32, Schema.TypeName.INT64,
            Schema.TypeName.FLOAT, Schema.TypeName.DOUBLE, Schema.TypeName.DECIMAL ->
                NumericValue(BigDecimal(config.value.trim()))

            Schema.TypeName.STRING ->
                BytesValue(config.value.toByteArray(StandardCharsets.UTF_8))

            else -> throw IllegalArgumentException(
                "the type ${fieldType.typeName} of predicate column [${config.column}] does not support pushdown yet, " +
                    "only numeric (byte/short/int/long/float/double) and string columns are supported"
            )
        }
    }
    return HivePredicate(config.column.trim(), op, fieldType, value)
}
