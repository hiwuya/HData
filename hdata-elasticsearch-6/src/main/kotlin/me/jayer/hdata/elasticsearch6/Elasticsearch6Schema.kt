package me.jayer.hdata.elasticsearch6

import org.apache.beam.sdk.schemas.Schema
import java.io.Serializable

/**
 * Elasticsearch `_source` 字段的逻辑类型，对齐 Beam schema 类型。
 */
enum class EsFieldType {
    STRING, INT32, INT64, DOUBLE, BOOLEAN, DATETIME, BYTES
}

/**
 * 单个输出/输入字段：`name:TYPE`。
 */
data class EsField(val name: String, val type: EsFieldType) : Serializable

/**
 * 把配置里的 `name:type` 列表解析成 [EsField]，类型名不区分大小写。
 */
fun parseSchemaFields(specs: List<String>): List<EsField> =
    specs.map { spec ->
        val (name, type) = spec.split(":", limit = 2)
        EsField(name.trim(), EsFieldType.valueOf(type.trim().uppercase()))
    }

/**
 * 由字段列表构建 Beam schema，全部声明为可空，避免 `_source` 缺字段时 NPE。
 */
fun buildSchema(fields: List<EsField>): Schema =
    Schema.builder().apply {
        fields.forEach { f ->
            when (f.type) {
                EsFieldType.STRING -> addNullableField(f.name, Schema.FieldType.STRING)
                EsFieldType.INT32 -> addNullableField(f.name, Schema.FieldType.INT32)
                EsFieldType.INT64 -> addNullableField(f.name, Schema.FieldType.INT64)
                EsFieldType.DOUBLE -> addNullableField(f.name, Schema.FieldType.DOUBLE)
                EsFieldType.BOOLEAN -> addNullableField(f.name, Schema.FieldType.BOOLEAN)
                EsFieldType.DATETIME -> addNullableField(f.name, Schema.FieldType.DATETIME)
                EsFieldType.BYTES -> addNullableField(f.name, Schema.FieldType.BYTES)
            }
        }
    }.build()

/** 没给 `schema_fields` 时读端的默认 schema：整条 `_source` 以 JSON 字符串输出。 */
val DOCUMENT_SCHEMA: Schema = Schema.builder()
    .addNullableStringField("document")
    .build()

/**
 * 把任意运行时值按字段类型转成 Elasticsearch 能序列化的 JSON 友好值
 * （DATETIME -> epoch millis，BYTES -> base64）。
 */
fun esValue(type: EsFieldType, raw: Any?): Any? {
    if (raw == null) {
        return null
    }
    return when (type) {
        EsFieldType.STRING -> raw.toString()
        EsFieldType.INT32 -> (raw as? Number)?.toInt() ?: raw.toString().toIntOrNull()
        EsFieldType.INT64 -> (raw as? Number)?.toLong() ?: raw.toString().toLongOrNull()
        EsFieldType.DOUBLE -> (raw as? Number)?.toDouble() ?: raw.toString().toDoubleOrNull()
        EsFieldType.BOOLEAN -> raw as? Boolean ?: raw.toString().toBoolean()
        EsFieldType.DATETIME -> when (raw) {
            is org.joda.time.Instant -> raw.millis
            is java.time.Instant -> raw.toEpochMilli()
            is Number -> raw.toLong()
            is String -> org.joda.time.Instant.parse(raw).millis
            else -> null
        }
        EsFieldType.BYTES -> when (raw) {
            is ByteArray -> java.util.Base64.getEncoder().encodeToString(raw)
            is String -> raw
            else -> null
        }
    }
}
