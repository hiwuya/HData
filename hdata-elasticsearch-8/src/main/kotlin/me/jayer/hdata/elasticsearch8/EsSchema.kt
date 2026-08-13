package me.jayer.hdata.elasticsearch8

import org.apache.beam.sdk.schemas.Schema

/**
 * 把 `schema_fields` 里的 `"name:TYPE"` 条目解析成 `(字段名, 类型)` 列表。
 *
 * 类型取值：STRING / INT32 / INT64 / DOUBLE / BOOLEAN / DATETIME / BYTES。
 */
internal fun parseSchemaFields(fields: List<String>): List<Pair<String, String>> =
    fields.map { spec ->
        val parts = spec.split(":", limit = 2)
        require(parts.size == 2) { "schema_fields 字段格式应为 name:type, 实际: $spec" }
        parts[0].trim() to parts[1].trim().uppercase()
    }

/** 根据 `schema_fields` 构造输出/输入 Beam schema；为空时退化为单 `document`(STRING) 列。 */
internal fun buildSchema(fields: List<String>): Schema {
    if (fields.isEmpty()) {
        return Schema.builder().addNullableField("document", Schema.FieldType.STRING).build()
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
    return when (type) {
        "STRING" -> value.toString()
        "INT32" -> when (value) {
            is Number -> value.toInt()
            is String -> value.toIntOrNull()
            else -> null
        }
        "INT64" -> when (value) {
            is Number -> value.toLong()
            is String -> value.toLongOrNull()
            else -> null
        }
        "DOUBLE" -> when (value) {
            is Number -> value.toDouble()
            is String -> value.toDoubleOrNull()
            else -> null
        }
        "BOOLEAN" -> when (value) {
            is Boolean -> value
            is String -> value.toBooleanStrictOrNull()
            else -> null
        }
        "DATETIME" -> when (value) {
            is Number -> org.joda.time.Instant.ofEpochMilli(value.toLong())
            is String -> runCatching { org.joda.time.Instant.parse(value) }.getOrNull()
            else -> null
        }
        "BYTES" -> when (value) {
            is ByteArray -> value
            is String -> runCatching { java.util.Base64.getDecoder().decode(value) }.getOrNull()
            else -> null
        }
        else -> value.toString()
    }
}

/** 写：把 Beam Row 里的字段值转换成 JSON 友好的类型。 */
internal fun toJsonValue(value: Any?, type: String): Any? {
    if (value == null) return null
    return when (type) {
        "DATETIME" -> value.toString()
        "BYTES" -> when (value) {
            is ByteArray -> java.util.Base64.getEncoder().encodeToString(value)
            else -> value.toString()
        }
        else -> value
    }
}
