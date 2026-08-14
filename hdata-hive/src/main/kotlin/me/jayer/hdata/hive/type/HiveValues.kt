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
 * 值层面的换算：分区字面量 <-> 类型化的值，以及写入端把上游的 Row 值对齐到表的列类型。
 *
 * @author wuya
 */
object HiveValues {

    /** Hive 文本格式里表示 null 的默认字面量。 */
    const val DEFAULT_NULL_FORMAT = "\\N"

    /**
     * 分区目录名里的值 -> 类型化的值。
     *
     * 分区列的值**不在数据文件里**，只存在于目录名 `dt=2024-01-01` 上，读取时要按分区列的类型
     * 还原出来再补进每一行。`__HIVE_DEFAULT_PARTITION__` 还原成 null。
     */
    fun fromPartitionLiteral(literal: String, fieldType: Schema.FieldType): Any? {
        if (PartitionNames.isDefaultPartition(literal)) {
            return null
        }
        return parseString(literal, fieldType)
    }

    /**
     * 类型化的值 -> 分区目录名里的值。null 与空串都落到 `__HIVE_DEFAULT_PARTITION__`
     * （转义交给 [PartitionNames.escapePathName]）。
     */
    fun toPartitionLiteral(value: Any?): String = when (value) {
        null -> PartitionNames.DEFAULT_PARTITION
        is ByteArray -> throw IllegalArgumentException("binary 类型不能作为分区列")
        is Instant -> value.toString()
        else -> value.toString()
    }

    /**
     * 字符串 -> 类型化的值，文本格式的读取和分区值还原共用。
     *
     * 解析失败返回 null 而不是抛异常：Hive 自己的 LazySimpleSerDe 就是这个语义
     * （脏数据的那一列变 NULL，不是整个作业挂掉），跟着它走才能读得动线上那些历史表。
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
                else -> throw IllegalArgumentException("暂不支持从字符串解析的逻辑类型: $fieldType")
            }

            else -> throw IllegalArgumentException("暂不支持从字符串解析的类型: $fieldType")
        }
    } catch (e: NumberFormatException) {
        null
    } catch (e: DateTimeParseException) {
        null
    }

    /** Hive 的 boolean 字面量只认 `true`（忽略大小写），其余一律 false。 */
    private fun parseBoolean(text: String): Boolean = text.trim().equals("true", ignoreCase = true)

    /** Hive 的 timestamp 字面量是 `yyyy-MM-dd HH:mm:ss[.fffffffff]`，中间是空格不是 `T`。 */
    fun parseLocalDateTime(text: String): LocalDateTime =
        LocalDateTime.parse(if (text.length > 10 && text[10] == ' ') text.replaceFirst(' ', 'T') else text)

    private fun parseInstant(text: String): Instant = try {
        Instant.parse(text)
    } catch (e: DateTimeParseException) {
        // 没有时区后缀时按 UTC 解释，与 Hive 的 timestamp with local time zone 在 UTC 会话下一致
        parseLocalDateTime(text).toInstant(ZoneOffset.UTC)
    }

    /**
     * 把上游的一行对齐到目标 schema：按**列名**取值（大小写不敏感，Hive 的列名一律小写），
     * 目标表里有而上游没有的列补 null。
     *
     * 按名字而不是按下标对齐是有意的：写入端拿到的 Row 来自上游任意 transform，
     * 字段顺序与 Hive 表一致纯属巧合，按下标写会静默地把数据写错列。
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
     * 把一个值换算成目标 Beam 类型。
     *
     * 只做**不丢信息**的换算加两条明确的例外：任何类型都可以写成 `string`，
     * 字符串可以按目标类型解析回去。数值之间的窄化（`bigint` -> `int`）超出范围时直接抛，
     * 不做静默截断——同步工具最怕的就是"数据到了但对不上"。
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
                require(it in Byte.MIN_VALUE..Byte.MAX_VALUE) { "$it 超出 tinyint 范围" }
                it.toByte()
            }

            Schema.TypeName.INT16 -> toExactLong(value, fieldType).let {
                require(it in Short.MIN_VALUE..Short.MAX_VALUE) { "$it 超出 smallint 范围" }
                it.toShort()
            }

            Schema.TypeName.INT32 -> toExactLong(value, fieldType).let {
                require(it in Int.MIN_VALUE..Int.MAX_VALUE) { "$it 超出 int 范围" }
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
            // Instant -> 墙上时间要挑一个时区，固定用 UTC 而不是 JVM 默认时区：
            // 默认时区会让同一份数据在不同机器上写出不同结果
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
            require(d == Math.floor(d) && !d.isInfinite()) { "$value 不是整数，无法写入 ${fieldType.typeName}" }
            d.toLong()
        }

        is Boolean -> if (value) 1L else 0L
        is String -> value.trim().toLong()
        else -> typeError(value, fieldType)
    }

    private fun typeError(value: Any, fieldType: Schema.FieldType): Nothing =
        throw IllegalArgumentException("无法把 ${value.javaClass.name} 换算成 $fieldType")
}
