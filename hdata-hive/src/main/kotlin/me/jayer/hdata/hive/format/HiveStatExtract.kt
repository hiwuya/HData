package me.jayer.hdata.hive.format

import org.apache.beam.sdk.schemas.Schema
import org.apache.orc.ColumnStatistics
import org.apache.orc.DecimalColumnStatistics
import org.apache.orc.DoubleColumnStatistics
import org.apache.orc.IntegerColumnStatistics
import org.apache.orc.StringColumnStatistics
import org.apache.orc.TypeDescription
import org.apache.parquet.column.statistics.Statistics
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import java.math.BigDecimal
import java.math.BigInteger
import java.math.RoundingMode
import java.nio.charset.StandardCharsets

/** ORC 顶层列在行批中的序号，以及它在扁平类型树中的真实 column id。 */
internal data class OrcColumnRef(
    val ordinal: Int,
    val id: Int,
    val type: TypeDescription,
)

internal fun orcTopLevelColumn(
    schema: TypeDescription,
    column: String,
    fallbackOrdinal: Int = -1,
): OrcColumnRef? {
    val named = schema.fieldNames.indexOfFirst { it.equals(column, ignoreCase = true) }
    val ordinal = if (named >= 0) named else fallbackOrdinal
    if (ordinal !in schema.children.indices) return null
    val type = schema.children[ordinal]
    return OrcColumnRef(ordinal, type.id, type)
}

/** Parquet 顶层 primitive 列的 Group 序号及其在 row-group leaf columns 中的序号。 */
internal data class ParquetColumnRef(
    val ordinal: Int,
    val leafOrdinal: Int,
    val type: PrimitiveType,
)

internal fun parquetTopLevelColumn(schema: MessageType, column: String): ParquetColumnRef? {
    val ordinal = schema.fields.indexOfFirst { it.name.equals(column, ignoreCase = true) }
    if (ordinal < 0) return null
    val type = schema.getType(ordinal) as? PrimitiveType ?: return null
    val actualName = type.name
    val leafOrdinal = schema.columns.indexOfFirst { descriptor ->
        descriptor.path.size == 1 && descriptor.path[0].equals(actualName, ignoreCase = true)
    }
    if (leafOrdinal < 0) return null
    return ParquetColumnRef(ordinal, leafOrdinal, type)
}

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
            val mn = numberToBigDecimal(stats.genericGetMin())
            val mx = numberToBigDecimal(stats.genericGetMax())
            if (mn != null && mx != null) {
                NumericValue(mn) to NumericValue(mx)
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

private fun numberToBigDecimal(raw: Any?): BigDecimal? = when (raw) {
    is BigDecimal -> raw
    is BigInteger -> raw.toBigDecimal()
    is Byte, is Short, is Int, is Long -> BigDecimal.valueOf((raw as Number).toLong())
    is Float, is Double -> BigDecimal.valueOf((raw as Number).toDouble())
    else -> null
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
