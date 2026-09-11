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

/** Ordinal of an ORC top-level column in the row batch, and its real column id in the flattened type tree. */
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

/** Group ordinal of a Parquet top-level primitive column and its ordinal among the row-group leaf columns. */
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

/** Takes the comparable min/max representation from ORC column statistics; returns null for unsupported types or missing
 * statistics (never skip/aggregate). */
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
            // ORC's DecimalColumnStatistics hands out a BigDecimal directly, but HiveDecimal strips trailing zeros, so normalize
            // again by the scale declared on the column, keeping DECIMAL(p,s) aggregate results consistent with the Parquet path.
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

/** Takes the comparable min/max representation from Parquet column statistics; returns null for unsupported types or missing
 * statistics. */
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
            // parquet 1.17 has no DecimalStatistics class: precision<=9 goes through INT32, <=18 through INT64 (statistics are
            // unscaled Long/Integer), and larger precisions through FIXED_LEN_BYTE_ARRAY (statistics are big-endian unscaled
            // bytes). Both must be converted back to BigDecimal by scale before comparing, otherwise the unscaled statistics and the predicate value differ in magnitude and compare wrongly.
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
 * Restores the comparable [ValueRepr] used by predicates/statistics into the real value inside a Beam Row.
 * Numbers are restored by the column's declared Beam type (INT64 -> Long, DOUBLE -> Double, DECIMAL -> BigDecimal), string -> String.
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
