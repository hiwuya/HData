package me.jayer.hdata.hive.type

import me.jayer.hdata.core.type.FieldTypes
import me.jayer.hdata.hive.metastore.PartitionNames
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.format.DateTimeParseException

/**
 * Value-level conversion: partition literal <-> typed value, plus aligning an upstream Row's values to the table's column types
 *
 * @author wuya
 */
object HiveValues {

    /** The literal Hive uses for null in text formats. */
    const val DEFAULT_NULL_FORMAT = "\\N"

    /**
     * A value in a partition directory name -> a typed value.
     *
     * Partition column values are **not in the data files**, they only exist in the directory name `dt=2024-01-01`; when reading
     * they are restored according to the partition column type and appended to every row. `__HIVE_DEFAULT_PARTITION__` becomes
     */
    fun fromPartitionLiteral(literal: String, fieldType: Schema.FieldType): Any? {
        if (PartitionNames.isDefaultPartition(literal)) {
            return null
        }
        return parseString(literal, fieldType)
    }

    /**
     * A typed value -> a value in a partition directory name. Both null and the empty string land on `__HIVE_DEFAULT_PARTITION__`
     * (escaping is [PartitionNames.escapePathName]'s job).
     */
    fun toPartitionLiteral(value: Any?): String = when (value) {
        null -> PartitionNames.DEFAULT_PARTITION
        is ByteArray -> throw IllegalArgumentException("binary columns cannot be used as partition columns")
        is Instant -> value.toString()
        else -> value.toString()
    }

    /**
     * String -> typed value, shared by text format reading and partition value restoration.
     *
     * A parse failure returns null instead of throwing: that is Hive's own LazySimpleSerDe semantics (the dirty column becomes
     * NULL rather than failing the whole job), and following it is what makes those legacy production tables readable.
     */
    fun parseString(text: String, fieldType: Schema.FieldType): Any? = try {
        when (fieldType.typeName) {
            Schema.TypeName.STRING -> text
            Schema.TypeName.BOOLEAN -> parseBoolean(text)
            Schema.TypeName.BYTE -> text.trim().toByte()
            Schema.TypeName.INT16 -> text.trim().toShort()
            Schema.TypeName.INT32 -> text.trim().toInt()
            Schema.TypeName.INT64 -> text.trim().toLong()
            Schema.TypeName.FLOAT -> text.trim().toFloat()
            Schema.TypeName.DOUBLE -> text.trim().toDouble()
            Schema.TypeName.DECIMAL -> BigDecimal(text.trim())
            Schema.TypeName.BYTES -> text.toByteArray()
            Schema.TypeName.LOGICAL_TYPE -> when (fieldType.withNullable(false)) {
                FieldTypes.DATE -> LocalDate.parse(text.trim())
                FieldTypes.DATETIME -> parseLocalDateTime(text.trim())
                FieldTypes.TIMESTAMP -> parseInstant(text.trim())
                FieldTypes.TIME -> LocalTime.parse(text.trim())
                else -> throw IllegalArgumentException("unsupported logical type to parse from a string: $fieldType")
            }

            else -> throw IllegalArgumentException("unsupported type to parse from a string: $fieldType")
        }
    } catch (e: NumberFormatException) {
        null
    } catch (e: DateTimeParseException) {
        null
    }

    /** Hive's boolean literal only recognizes `true` (case-insensitive); anything else is false. */
    private fun parseBoolean(text: String): Boolean = text.trim().equals("true", ignoreCase = true)

    /** Hive's timestamp literal is `yyyy-MM-dd HH:mm:ss[.fffffffff]`, with a space in the middle instead of `T`. */
    fun parseLocalDateTime(text: String): LocalDateTime =
        LocalDateTime.parse(if (text.length > 10 && text[10] == ' ') text.replaceFirst(' ', 'T') else text)

    private fun parseInstant(text: String): Instant = try {
        Instant.parse(text)
    } catch (e: DateTimeParseException) {
        // With no time zone suffix, interpret it as UTC, consistent with Hive's timestamp with local time zone under a UTC session
        parseLocalDateTime(text).toInstant(ZoneOffset.UTC)
    }

    /**
     * Aligns an upstream row to the target schema: values are taken by **column name** (case-insensitive, Hive column names are
     * always lowercase), and columns present in the target table but missing upstream are filled with null.
     *
     * Aligning by name rather than by index is deliberate: the Row the write side receives comes from an arbitrary upstream
     * transform, so a field order matching the Hive table is pure coincidence, and writing by index silently puts data into the
     */
    fun align(row: Row, targetSchema: Schema): Row {
        val sourceSchema = row.schema
        val builder = Row.withSchema(targetSchema)
        targetSchema.fields.forEach { field ->
            val sourceField = sourceSchema.fields.firstOrNull { it.name.equals(field.name, ignoreCase = true) }
            val value = sourceField?.let { row.getValue<Any?>(it.name) }
            builder.addValue(if (value == null) null else coerce(value, field.type))
        }
        return builder.build()
    }

    /**
     * Converts one value into the target Beam type.
     *
     * Only **lossless** conversions plus two explicit exceptions: any type may be written as `string`, and a string may be parsed
     * back according to the target type. Narrowing between numeric types (`bigint` -> `int`) throws when out of range instead of
     * truncating silently — the worst thing a sync tool can do is deliver data that does not match.
     */
    fun coerce(value: Any, fieldType: Schema.FieldType): Any? {
        val target = fieldType.withNullable(false)
        return when (target.typeName) {
            Schema.TypeName.STRING -> when (value) {
                is ByteArray -> String(value)
                else -> value.toString()
            }

            Schema.TypeName.BOOLEAN -> when (value) {
                is Boolean -> value
                is Number -> value.toLong() != 0L
                is String -> parseBoolean(value)
                else -> typeError(value, fieldType)
            }

            Schema.TypeName.BYTE -> toExactLong(value, fieldType).let {
                require(it in Byte.MIN_VALUE..Byte.MAX_VALUE) { "$it is out of range for tinyint" }
                it.toByte()
            }

            Schema.TypeName.INT16 -> toExactLong(value, fieldType).let {
                require(it in Short.MIN_VALUE..Short.MAX_VALUE) { "$it is out of range for smallint" }
                it.toShort()
            }

            Schema.TypeName.INT32 -> toExactLong(value, fieldType).let {
                require(it in Int.MIN_VALUE..Int.MAX_VALUE) { "$it is out of range for int" }
                it.toInt()
            }

            Schema.TypeName.INT64 -> toExactLong(value, fieldType)

            Schema.TypeName.FLOAT -> when (value) {
                is Number -> value.toFloat()
                is String -> value.trim().toFloat()
                else -> typeError(value, fieldType)
            }

            Schema.TypeName.DOUBLE -> when (value) {
                is Number -> value.toDouble()
                is String -> value.trim().toDouble()
                else -> typeError(value, fieldType)
            }

            Schema.TypeName.DECIMAL -> when (value) {
                is BigDecimal -> value
                is java.math.BigInteger -> BigDecimal(value)
                is Number -> BigDecimal(value.toString())
                is String -> BigDecimal(value.trim())
                else -> typeError(value, fieldType)
            }

            Schema.TypeName.BYTES -> when (value) {
                is ByteArray -> value
                is ByteBuffer -> ByteArray(value.remaining()).also { value.duplicate().get(it) }
                is String -> value.toByteArray()
                else -> typeError(value, fieldType)
            }

            Schema.TypeName.ARRAY, Schema.TypeName.ITERABLE -> {
                val elementType = target.collectionElementType!!
                (value as? Iterable<*> ?: typeError(value, fieldType))
                    .map { element -> element?.let { coerce(it, elementType) } }
            }

            Schema.TypeName.MAP -> {
                val keyType = target.mapKeyType!!
                val valueType = target.mapValueType!!
                (value as? Map<*, *> ?: typeError(value, fieldType))
                    .entries
                    .associate { (k, v) ->
                        coerce(k!!, keyType) to v?.let { coerce(it, valueType) }
                    }
            }

            Schema.TypeName.ROW -> when (value) {
                is Row -> align(value, target.rowSchema!!)
                else -> typeError(value, fieldType)
            }

            Schema.TypeName.LOGICAL_TYPE -> coerceLogical(value, target)

            else -> typeError(value, fieldType)
        }
    }

    private fun coerceLogical(value: Any, target: Schema.FieldType): Any = when (target) {
        FieldTypes.DATE -> when (value) {
            is LocalDate -> value
            is LocalDateTime -> value.toLocalDate()
            is Instant -> value.atZone(ZoneOffset.UTC).toLocalDate()
            is String -> LocalDate.parse(value.trim())
            else -> typeError(value, target)
        }

        FieldTypes.DATETIME -> when (value) {
            is LocalDateTime -> value
            is LocalDate -> value.atStartOfDay()
            // Instant -> wall-clock time needs a time zone; use UTC rather than the JVM default, which would make the same data
            // produce different results on different machines
            is Instant -> LocalDateTime.ofInstant(value, ZoneOffset.UTC)
            is String -> parseLocalDateTime(value.trim())
            else -> typeError(value, target)
        }

        FieldTypes.TIMESTAMP -> when (value) {
            is Instant -> value
            is LocalDateTime -> value.toInstant(ZoneOffset.UTC)
            is LocalDate -> value.atStartOfDay().toInstant(ZoneOffset.UTC)
            is org.joda.time.Instant -> Instant.ofEpochMilli(value.millis)
            is String -> parseInstant(value.trim())
            else -> typeError(value, target)
        }

        FieldTypes.TIME -> when (value) {
            is LocalTime -> value
            is String -> LocalTime.parse(value.trim())
            else -> typeError(value, target)
        }

        else -> typeError(value, target)
    }

    private fun toExactLong(value: Any, fieldType: Schema.FieldType): Long = when (value) {
        is Long -> value
        is Int -> value.toLong()
        is Short -> value.toLong()
        is Byte -> value.toLong()
        is BigDecimal -> value.longValueExact()
        is java.math.BigInteger -> value.longValueExact()
        is Number -> {
            val d = value.toDouble()
            require(d == Math.floor(d) && !d.isInfinite()) { "$value is not an integer, cannot write it into ${fieldType.typeName}" }
            d.toLong()
        }

        is Boolean -> if (value) 1L else 0L
        is String -> value.trim().toLong()
        else -> typeError(value, fieldType)
    }

    private fun typeError(value: Any, fieldType: Schema.FieldType): Nothing =
        throw IllegalArgumentException("cannot convert ${value.javaClass.name} into $fieldType")
}
