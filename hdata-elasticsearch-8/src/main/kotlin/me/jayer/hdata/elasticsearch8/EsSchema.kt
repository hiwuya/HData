package me.jayer.hdata.elasticsearch8

import org.apache.beam.sdk.schemas.Schema
import java.math.BigDecimal

/**
 * 把 `schema_fields` 里的 `"name:TYPE"` 条目解析成 `(字段名, 类型)` 列表。
 *
 * 类型取值：STRING / INT32 / INT64 / DOUBLE / BOOLEAN / DATETIME / BYTES。
 */
internal fun parseSchemaFields(fields: List<String>): List<Pair<String, String>> =
    fields.map { spec ->
        val parts = spec.split(":", limit = 2)
        require(parts.size == 2) { "schema_fields 字段格式应为 name:type, 实际: $spec" }
        require(parts[0].isNotBlank()) { "schema_fields 字段名不能为空: $spec" }
        require(parts[1].isNotBlank()) { "schema_fields 类型不能为空: $spec" }
        parts[0].trim() to parts[1].trim().uppercase()
    }

/**
 * 不声明 `schema_fields` 时的单列名，读写两端共用。
 *
 * 早先读端产出 `document`、写端却找 `value`，读出来的数据一行也写不回去——
 * 列只有一个名字，放在一处才不会再次分叉。
 */
const val DOCUMENT_FIELD = "document"

/** 根据 `schema_fields` 构造输出/输入 Beam schema；为空时退化为单 `document`(STRING) 列。 */
internal fun buildSchema(fields: List<String>): Schema {
    if (fields.isEmpty()) {
        return Schema.builder().addNullableField(DOCUMENT_FIELD, Schema.FieldType.STRING).build()
    }
    val builder = Schema.builder()
    parseSchemaFields(fields).forEach { (name, type) ->
        builder.addNullableField(name, fieldType(type))
    }
    return builder.build()
}

internal fun fieldType(type: String): Schema.FieldType = when (type) {
    "STRING" -> Schema.FieldType.STRING
    "INT32" -> Schema.FieldType.INT32
    "INT64" -> Schema.FieldType.INT64
    "DOUBLE" -> Schema.FieldType.DOUBLE
    "BOOLEAN" -> Schema.FieldType.BOOLEAN
    "DATETIME" -> Schema.FieldType.DATETIME
    "BYTES" -> Schema.FieldType.BYTES
    else -> throw IllegalArgumentException("不支持的 schema_fields 类型: $type")
}

/** 读：把 ES `_source` 里的值转换成 Beam Row 能接受的类型。 */
internal fun convertValue(value: Any?, type: String): Any? {
    if (value == null) return null
    return try {
        when (type) {
            "STRING" -> value.toString()
            "INT32" -> decimal(value).intValueExact()
            "INT64" -> decimal(value).longValueExact()
            "DOUBLE" -> decimal(value).toDouble().also { require(it.isFinite()) { "超出 DOUBLE 有限范围" } }
            "BOOLEAN" -> when (value) {
                is Boolean -> value
                is String -> value.toBooleanStrict()
                else -> throw IllegalArgumentException("不是 BOOLEAN")
            }
            "DATETIME" -> when (value) {
                is Number -> org.joda.time.Instant.ofEpochMilli(decimal(value).longValueExact())
                is String -> org.joda.time.Instant.parse(value)
                else -> throw IllegalArgumentException("不是 DATETIME")
            }
            "BYTES" -> when (value) {
                is ByteArray -> value
                is String -> java.util.Base64.getDecoder().decode(value)
                else -> throw IllegalArgumentException("不是 BYTES/base64 字符串")
            }
            else -> throw IllegalArgumentException("不支持的 schema_fields 类型: $type")
        }
    } catch (e: Exception) {
        throw IllegalArgumentException("值[$value]无法转换为 $type", e)
    }
}

/** 写：把 Beam Row 里的字段值转换成 JSON 友好的类型。 */
internal fun toJsonValue(value: Any?, type: String): Any? {
    if (value == null) return null
    return when (type) {
        "STRING" -> value.toString()
        "INT32" -> convertValue(value, type)
        "INT64" -> convertValue(value, type)
        "DOUBLE" -> convertValue(value, type)
        "BOOLEAN" -> convertValue(value, type)
        "DATETIME" -> when (value) {
            is org.joda.time.Instant -> value.toString()
            else -> throw IllegalArgumentException("值[$value]不是 Beam DATETIME")
        }
        "BYTES" -> when (value) {
            is ByteArray -> java.util.Base64.getEncoder().encodeToString(value)
            else -> throw IllegalArgumentException("值[$value]不是 Beam BYTES")
        }
        else -> throw IllegalArgumentException("不支持的 schema_fields 类型: $type")
    }
}

private fun decimal(value: Any): BigDecimal = when (value) {
    is BigDecimal -> value
    is Number, is String -> value.toString().toBigDecimal()
    else -> throw IllegalArgumentException("不是数字")
}
