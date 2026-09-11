package me.jayer.hdata.elasticsearch6

import org.apache.beam.sdk.schemas.Schema
import java.io.Serializable
import java.math.BigDecimal

/**
 * The logical type of an Elasticsearch `_source` field, aligned with Beam schema types.
 */
enum class EsFieldType {
    STRING, INT32, INT64, DOUBLE, BOOLEAN, DATETIME, BYTES
}

/**
 * A single output/input field: `name:TYPE`.
 */
data class EsField(val name: String, val type: EsFieldType) : Serializable

/**
 * Parses the `name:type` list from the config into [EsField]s; type names are case-insensitive.
 */
fun parseSchemaFields(specs: List<String>): List<EsField> =
    specs.map { spec ->
        // When the colon is missing, destructuring would throw a bare IndexOutOfBoundsException that gives no hint that the config is malformed.
        val parts = spec.split(":", limit = 2)
        require(parts.size == 2) { "a schema_fields entry should have the format name:type, but got: $spec" }
        require(parts[0].isNotBlank()) { "the field name of a schema_fields entry must not be empty: $spec" }
        val type = parts[1].trim().uppercase()
        val fieldType = EsFieldType.entries.firstOrNull { it.name == type }
            ?: throw IllegalArgumentException(
                "unsupported schema_fields type: ${parts[1].trim()}, valid options are ${EsFieldType.entries.joinToString { it.name }}"
            )
        EsField(parts[0].trim(), fieldType)
    }

/**
 * Builds a Beam schema from the field list, declaring all fields nullable to avoid an NPE when `_source` is missing a field.
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

/**
 * The single column name used when `schema_fields` is not declared, shared by the read and write sides.
 *
 * Earlier the read side produced `document` while the write side looked for `value`, so not a single row read out could
 * be written back — the column has only one name, and keeping it in one place is what prevents the two from diverging again.
 */
const val DOCUMENT_FIELD = "document"

/** The read side's default schema when `schema_fields` is not given: the entire `_source` is output as a JSON string. */
val DOCUMENT_SCHEMA: Schema = Schema.builder()
    .addNullableStringField(DOCUMENT_FIELD)
    .build()

/**
 * Converts an arbitrary runtime value, according to its field type, into a JSON-friendly value Elasticsearch can
 * serialize (DATETIME -> epoch millis, BYTES -> base64).
 */
fun esValue(type: EsFieldType, raw: Any?): Any? {
    if (raw == null) {
        return null
    }
    return try {
        when (type) {
            EsFieldType.STRING -> raw.toString()
            EsFieldType.INT32 -> decimal(raw).intValueExact()
            EsFieldType.INT64 -> decimal(raw).longValueExact()
            EsFieldType.DOUBLE -> decimal(raw).toDouble().also { require(it.isFinite()) { "out of the finite DOUBLE range" } }
            EsFieldType.BOOLEAN -> when (raw) {
                is Boolean -> raw
                is String -> raw.toBooleanStrict()
                else -> throw IllegalArgumentException("not a BOOLEAN")
            }
            EsFieldType.DATETIME -> when (raw) {
                is org.joda.time.Instant -> raw.millis
                is java.time.Instant -> raw.toEpochMilli()
                is Number -> decimal(raw).longValueExact()
                is String -> org.joda.time.Instant.parse(raw).millis
                else -> throw IllegalArgumentException("not a DATETIME")
            }
            EsFieldType.BYTES -> when (raw) {
                is ByteArray -> java.util.Base64.getEncoder().encodeToString(raw)
                is String -> java.util.Base64.getDecoder().decode(raw).let { raw }
                else -> throw IllegalArgumentException("not a BYTES/base64 string")
            }
        }
    } catch (e: Exception) {
        throw IllegalArgumentException("value [$raw] cannot be converted to $type", e)
    }
}

/** Strictly converts an Elasticsearch `_source` value into the Beam Row type. */
fun esRowValue(type: EsFieldType, raw: Any?): Any? {
    if (raw == null) return null
    return when (type) {
        EsFieldType.DATETIME -> org.joda.time.Instant.ofEpochMilli(esValue(type, raw) as Long)
        EsFieldType.BYTES -> when (raw) {
            is ByteArray -> raw
            is String -> try {
                java.util.Base64.getDecoder().decode(raw)
            } catch (e: IllegalArgumentException) {
                throw IllegalArgumentException("value [$raw] cannot be converted to BYTES", e)
            }
            else -> throw IllegalArgumentException("value [$raw] cannot be converted to BYTES")
        }
        else -> esValue(type, raw)
    }
}

private fun decimal(value: Any): BigDecimal = when (value) {
    is BigDecimal -> value
    is Number, is String -> value.toString().toBigDecimal()
    else -> throw IllegalArgumentException("not a number")
}
