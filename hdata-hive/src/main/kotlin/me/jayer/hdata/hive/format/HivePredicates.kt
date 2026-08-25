package me.jayer.hdata.hive.format

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import java.io.Serializable
import java.math.BigDecimal
import java.nio.charset.StandardCharsets

/**
 * 读取端的谓词下推（predicate pushdown），对齐 Trino Hive 连接器的 `TupleDomain`：
 * 用 ORC stripe / Parquet row group 的列统计（min/max）跳过不可能命中谓词的单元，
 * 再在行级兜一层过滤保证结果正确。
 *
 * 与 Trino 一样只下推**数据列**上的简单比较谓词（AND 关系），分区列的裁剪仍走 `partition_filter`。
 * 下推能在文件统计层面跳过的有数值（byte/short/int/long/float/double）、decimal 与字符串列——
 * 这三类正好覆盖了绝大多数 `WHERE` 过滤；日期/时间戳/布尔/嵌套类型只走行级过滤，
 * 正确性不受影响。
 */

enum class PredicateOp {
    EQ, NEQ, GT, GTE, LT, LTE, IS_NULL, IS_NOT_NULL
}

/** 谓词值与列统计最小/最大值的统一可比较表示。 */
sealed interface ValueRepr : Serializable {
    fun compareTo(other: ValueRepr): Int
}

data class NumericValue(val v: BigDecimal) : ValueRepr {
    override fun compareTo(other: ValueRepr): Int =
        if (other is NumericValue) v.compareTo(other.v) else throw IllegalArgumentException("谓词类型不匹配：期望数值")
}

data class BytesValue(val v: ByteArray) : ValueRepr {
    override fun compareTo(other: ValueRepr): Int =
        if (other is BytesValue) unsignedCompare(v, other.v) else throw IllegalArgumentException("谓词类型不匹配：期望字符串")

    override fun equals(other: Any?): Boolean = other is BytesValue && v.contentEquals(other.v)
    override fun hashCode(): Int = v.contentHashCode()
}

/** 一个解析好的比较谓词。 */
data class HivePredicate(
    val column: String,
    val op: PredicateOp,
    val fieldType: Schema.FieldType,
    val value: ValueRepr?,
) : Serializable

/** 某个 stripe / row group 里某列的统计摘要；不可下推时不传（min/max 为 null）。 */
data class ColumnRangeStats(
    val min: ValueRepr?,
    val max: ValueRepr?,
    val hasNull: Boolean,
    /** 该单元整列是否全是 NULL；只有它为真时 `col IS NOT NULL` 才能整段跳过。 */
    val allNull: Boolean = false,
) : Serializable

object PredicateEvaluator {

    /** 行级过滤：保留当且仅当**所有**谓词都为 TRUE 的行（三值逻辑，未知即丢弃）。 */
    fun matches(row: Row, predicates: List<HivePredicate>): Boolean = predicates.all { matchesOne(row, it) }

    /**
     * 分区裁剪用：判断某列上的**常量值**（例如分区目录里还原出来的那个值）是否满足谓词。
     * 三值逻辑与行级一致——拿不准（未知）返回 false，即保守地"不裁剪、照常读"，绝不因为裁剪丢数据。
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
                    ?: throw IllegalStateException("无法把字段值 [$cell] 转成谓词可比较的表示")
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
     * 统计层跳过：只要**任意一个**谓词能证明整个单元不可能命中，就跳过该 stripe / row group。
     * 判定一律保守——拿不准就返回 false（不跳过、照常读），绝不会因为下推而丢数据。
     */
    fun canSkip(predicates: List<HivePredicate>, stats: Map<String, ColumnRangeStats>): Boolean =
        predicates.any { canSkipOne(it, stats[it.column]) }

    private fun canSkipOne(p: HivePredicate, s: ColumnRangeStats?): Boolean {
        if (s == null) return false
        return when (p.op) {
            // 单元里没有 NULL，那 `col IS NULL` 必然全不命中 -> 可跳过
            PredicateOp.IS_NULL -> !s.hasNull
            // `col IS NOT NULL` 只有当整列全是 NULL 才能跳过（否则里面藏着非 NULL 行）
            PredicateOp.IS_NOT_NULL -> s.allNull
            else -> {
                if (s.min == null || s.max == null || p.value == null) return false
                when (p.op) {
                    PredicateOp.EQ -> p.value.compareTo(s.min) < 0 || p.value.compareTo(s.max) > 0
                    // 整列 min==max==value 时 `!= value` 才全不命中
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

    /** 把行里的单元格值转成可比较表示；不支持的类型返回 null（行级过滤时按不可比较处理）。 */
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

/** 配置里的原始谓词（YAML 直接反序列化）。 */
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

/** 把配置里的 `op` 文本解析成 [PredicateOp]；非法操作符直接抛，避免默默退化。 */
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
            "不支持的谓词操作符: '$text'（可选: = != > >= < <= is null is not null）"
        )
    }
}

/** 把配置里的原始谓词解析成下推用的 [HivePredicate]；列类型不支持时在此显式报错。 */
fun parsePredicate(config: ConfigPredicate, fieldType: Schema.FieldType): HivePredicate {
    require(config.column.isNotBlank()) { "谓词缺少 column" }
    val op = parsePredicateOp(config.op)
    val value = if (op == PredicateOp.IS_NULL || op == PredicateOp.IS_NOT_NULL) {
        null
    } else {
        require(config.value.isNotBlank()) { "谓词 [$config] 的比较值不能为空" }
        when (fieldType.typeName) {
            Schema.TypeName.BYTE, Schema.TypeName.INT16, Schema.TypeName.INT32, Schema.TypeName.INT64,
            Schema.TypeName.FLOAT, Schema.TypeName.DOUBLE, Schema.TypeName.DECIMAL ->
                NumericValue(BigDecimal(config.value.trim()))

            Schema.TypeName.STRING ->
                BytesValue(config.value.toByteArray(StandardCharsets.UTF_8))

            else -> throw IllegalArgumentException(
                "谓词列 [${config.column}] 的类型 ${fieldType.typeName} 暂不支持下推，" +
                    "只支持数值(byte/short/int/long/float/double)与字符串(string)列"
            )
        }
    }
    return HivePredicate(config.column.trim(), op, fieldType, value)
}
