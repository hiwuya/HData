package me.jayer.hdata.hive.format

import org.apache.beam.sdk.schemas.Schema
import org.apache.orc.ColumnStatistics
import org.apache.orc.DecimalColumnStatistics
import org.apache.orc.DoubleColumnStatistics
import org.apache.orc.IntegerColumnStatistics
import org.apache.orc.StringColumnStatistics
import org.apache.parquet.column.statistics.Statistics
import org.apache.parquet.io.api.Binary
import java.math.BigDecimal
import java.math.BigInteger
import java.math.RoundingMode
import java.nio.charset.StandardCharsets

/** 从 ORC 列统计里取 min/max 的可比较表示；类型不支持或统计缺失返回 null（绝不跳过/聚合）。 */
internal fun orcColumnRange(colStats: ColumnStatistics, fieldType: Schema.FieldType, decimalScale: Int): Pair<ValueRepr?, ValueRepr?> =
    when (fieldType.typeName) {
        Schema.TypeName.BYTE, Schema.TypeName.INT16, Schema.TypeName.INT32, Schema.TypeName.INT64 -> {
            if (colStats is IntegerColumnStatistics) {
                NumericValue(BigDecimal(colStats.minimum)) to NumericValue(BigDecimal(colStats.maximum))
            } else {
                null to null
            }
        }

        Schema.TypeName.FLOAT, Schema.TypeName.DOUBLE -> {
            if (colStats is DoubleColumnStatistics) {
                NumericValue(BigDecimal(colStats.minimum)) to NumericValue(BigDecimal(colStats.maximum))
            } else {
                null to null
            }
        }

        Schema.TypeName.DECIMAL -> {
            // ORC 的 DecimalColumnStatistics 直接给出 BigDecimal，但 HiveDecimal 会去掉末尾 0，
            // 所以这里按列声明的 scale 重新归一，保证 DECIMAL(p,s) 聚合结果与 Parquet 路径一致。
            if (colStats is DecimalColumnStatistics) {
                val mn = colStats.minimum?.bigDecimalValue()?.setScale(decimalScale, RoundingMode.UNNECESSARY)
                val mx = colStats.maximum?.bigDecimalValue()?.setScale(decimalScale, RoundingMode.UNNECESSARY)
                if (mn != null && mx != null) {
                    NumericValue(mn) to NumericValue(mx)
                } else {
                    null to null
                }
            } else {
                null to null
            }
        }

        Schema.TypeName.STRING -> {
            if (colStats is StringColumnStatistics) {
                BytesValue(colStats.minimum.toByteArray(StandardCharsets.UTF_8)) to
                    BytesValue(colStats.maximum.toByteArray(StandardCharsets.UTF_8))
            } else {
                null to null
            }
        }

        else -> null to null
    }

/** 从 Parquet 列统计里取 min/max 的可比较表示；类型不支持或统计缺失返回 null。 */
internal fun parquetColumnRange(
    stats: Statistics<*>,
    fieldType: Schema.FieldType,
    decimalScale: Int,
): Pair<ValueRepr?, ValueRepr?> =
    when (fieldType.typeName) {
        Schema.TypeName.BYTE, Schema.TypeName.INT16, Schema.TypeName.INT32, Schema.TypeName.INT64,
        Schema.TypeName.FLOAT, Schema.TypeName.DOUBLE -> {
            val mn = (stats.genericGetMin() as? Number)?.toDouble()
            val mx = (stats.genericGetMax() as? Number)?.toDouble()
            if (mn != null && mx != null) {
                NumericValue(BigDecimal(mn)) to NumericValue(BigDecimal(mx))
            } else {
                null to null
            }
        }

        Schema.TypeName.STRING -> {
            val mn = (stats.genericGetMin() as? Binary)?.toStringUsingUTF8()
            val mx = (stats.genericGetMax() as? Binary)?.toStringUsingUTF8()
            if (mn != null && mx != null) {
                BytesValue(mn.toByteArray(StandardCharsets.UTF_8)) to
                    BytesValue(mx.toByteArray(StandardCharsets.UTF_8))
            } else {
                null to null
            }
        }

        Schema.TypeName.DECIMAL -> {
            // parquet 1.17 没有 DecimalStatistics 这个类：precision<=9 走 INT32、<=18 走 INT64
            // （统计是未缩放的 Long/Integer），更大的 precision 走 FIXED_LEN_BYTE_ARRAY（统计是 big-endian
            // 未缩放字节）。两种都要按 scale 换回 BigDecimal 再比较，否则未缩放值和谓词值量纲不同会误判。
            val mn = decimalToBigDecimal(stats.genericGetMin(), decimalScale)
            val mx = decimalToBigDecimal(stats.genericGetMax(), decimalScale)
            if (mn != null && mx != null) {
                NumericValue(mn) to NumericValue(mx)
            } else {
                null to null
            }
        }

        else -> null to null
    }

internal fun decimalToBigDecimal(raw: Any?, scale: Int): BigDecimal? = when (raw) {
    is Number -> BigDecimal(raw.toLong()).movePointLeft(scale)
    is Binary -> BigDecimal(BigInteger(raw.bytes), scale)
    else -> null
}

/**
 * 把谓词/统计用的可比较表示 [ValueRepr] 还原成 Beam Row 里真正的值。
 * 数值按列声明的 Beam 类型还原（INT64→Long、DOUBLE→Double、DECIMAL→BigDecimal），字符串→String。
 */
internal fun reprToValue(repr: ValueRepr?, fieldType: Schema.FieldType): Any? {
    if (repr == null) return null
    return when (repr) {
        is NumericValue -> {
            val v = repr.v
            when (fieldType.typeName) {
                Schema.TypeName.INT64 -> v.toLong()
                Schema.TypeName.INT32, Schema.TypeName.INT16, Schema.TypeName.BYTE -> v.toInt()
                Schema.TypeName.FLOAT -> v.toFloat()
                Schema.TypeName.DOUBLE -> v.toDouble()
                else -> v
            }
        }

        is BytesValue -> String(repr.v, StandardCharsets.UTF_8)
    }
}
