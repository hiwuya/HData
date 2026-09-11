package me.jayer.hdata.elasticsearch8

import org.apache.beam.sdk.schemas.Schema
import java.math.BigDecimal

/**
 * Parses `schema_fields` `name:TYPE` entries into field names and types.
 */
internal fun parseSchemaFields(fields: List<String>): List<Pair<String, String>> =
    fields.map { spec ->
        val parts = spec.split(":", limit = 2)
        require(parts.size == 2) { "schema_fields entry must be name:type, received: $spec" }
        require(parts[0].isNotBlank()) { "schema_fields field name must not be blank: $spec" }
        require(parts[1].isNotBlank()) { "schema_fields type must not be blank: $spec" }
        parts[0].trim() to parts[1].trim().uppercase()
    }

/**
 * Shared single-column name when `schema_fields` is omitted.
 */
const val DOCUMENT_FIELD = "document"

/** Builds input/output Beam schema from `schema_fields`, or a single STRING `document` field. */
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
    else -> throw IllegalArgumentException("unsupported schema_fields type: $type")
}

/** Converts ES `_source` values to Beam Row-compatible types. */
internal fun convertValue(value: Any?, type: String): Any? {
    if (value == null) return null
    return try {
        when (type) {
            "STRING" -> value.toString()
            "INT32" -> decimal(value).intValueExact()
            "INT64" -> decimal(value).longValueExact()
            "DOUBLE" -> decimal(value).toDouble().also { require(it.isFinite()) { "value exceeds the finite DOUBLE range" } }
            "BOOLEAN" -> when (value) {
                is Boolean -> value
                is String -> value.toBooleanStrict()
                else -> throw IllegalArgumentException("value is not BOOLEAN")
            }
            "DATETIME" -> when (value) {
                is Number -> org.joda.time.Instant.ofEpochMilli(decimal(value).longValueExact())
                is String -> org.joda.time.Instant.parse(value)
                else -> throw IllegalArgumentException("value is not DATETIME")
            }
            "BYTES" -> when (value) {
                is ByteArray -> value
                is String -> java.util.Base64.getDecoder().decode(value)
                else -> throw IllegalArgumentException("value is not BYTES or a base64 string")
            }
            else -> throw IllegalArgumentException("unsupported schema_fields type: $type")
        }
    } catch (e: Exception) {
        throw IllegalArgumentException("value [$value] cannot be converted to $type", e)
    }
}

/** Converts Beam Row values to JSON-compatible types for writes. */
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
            else -> throw IllegalArgumentException("value [$value] is not Beam DATETIME")
        }
        "BYTES" -> when (value) {
            is ByteArray -> java.util.Base64.getEncoder().encodeToString(value)
            else -> throw IllegalArgumentException("value [$value] is not Beam BYTES")
        }
        else -> throw IllegalArgumentException("unsupported schema_fields type: $type")
    }
}

private fun decimal(value: Any): BigDecimal = when (value) {
    is BigDecimal -> value
    is Number, is String -> value.toString().toBigDecimal()
    else -> throw IllegalArgumentException("value is not numeric")
}
