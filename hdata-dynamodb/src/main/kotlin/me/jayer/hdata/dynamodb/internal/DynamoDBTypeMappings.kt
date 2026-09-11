package me.jayer.hdata.dynamodb.internal

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import java.math.BigDecimal

/**
 * Bidirectional mapping between DynamoDB [AttributeValue] and Beam [Schema] types.
 *
 * DynamoDB attribute types:
 * - S (String) -> Beam STRING
 * - N (Number) -> Beam DECIMAL (numbers can have arbitrary precision)
 * - B (Binary) -> Beam BYTES
 * - BOOL       -> Beam BOOLEAN
 * - NUL        -> Beam nullable field (value = null)
 * - L (List)   -> Beam ARRAY<STRING>
 * - M (Map)    -> Beam MAP<STRING, STRING>
 * - SS (String Set)  -> Beam ARRAY<STRING>
 * - NS (Number Set)  -> Beam ARRAY<STRING>
 * - BS (Binary Set)  -> Beam ARRAY<BYTES>
 *
 * @author wuya
 */
object DynamoDBTypeMappings {

    /**
     * Derives a Beam schema from the first DynamoDB items. The schema is built by scanning
     * all attributes across all items and picking the most general Beam type for each attribute.
     *
     * All fields are nullable since DynamoDB is schemaless and any attribute may be absent.
     */
    fun deriveSchema(items: List<Map<String, AttributeValue>>): Schema {
        if (items.isEmpty()) {
            return Schema.builder().build()
        }

        // Collect all attribute names and their representative Beam types.
        val attributeTypes = mutableMapOf<String, Schema.FieldType>()

        for (item in items) {
            for ((key, value) in item) {
                val beamType = toBeamType(value)
                val existing = attributeTypes[key]
                if (existing == null) {
                    attributeTypes[key] = beamType
                } else {
                    // Promote to the wider type if they differ.
                    attributeTypes[key] = promoteType(existing, beamType)
                }
            }
        }

        val builder = Schema.builder()
        for ((name, type) in attributeTypes.toSortedMap()) {
            builder.addNullableField(name, type)
        }
        return builder.build()
    }

    /** Converts a DynamoDB item to a Beam Row following the given schema. */
    fun itemToRow(item: Map<String, AttributeValue>, schema: Schema): Row {
        val builder = Row.withSchema(schema)
        for (i in 0 until schema.fieldCount) {
            val field = schema.getField(i)
            val value = item[field.name]
            if (value == null || value.nul() == true) {
                builder.addValue(null)
            } else {
                builder.addValue(toBeamValue(value, field.type))
            }
        }
        return builder.build()
    }

    /** Converts a Beam Row to a DynamoDB item (Map of attribute name to AttributeValue). */
    fun rowToItem(row: Row): Map<String, AttributeValue> {
        val item = mutableMapOf<String, AttributeValue>()
        for (i in 0 until row.schema.fieldCount) {
            val field = row.schema.getField(i)
            val value = row.getValue<Any?>(i)
            item[field.name] = toAttributeValue(value, field.type)
        }
        return item
    }

    // ---------- Beam type derivation ----------

    private fun toBeamType(value: AttributeValue): Schema.FieldType = when (value.type()) {
        AttributeValue.Type.S -> Schema.FieldType.STRING
        AttributeValue.Type.N -> Schema.FieldType.DECIMAL
        AttributeValue.Type.B -> Schema.FieldType.BYTES
        AttributeValue.Type.BOOL -> Schema.FieldType.BOOLEAN
        AttributeValue.Type.NUL -> Schema.FieldType.STRING // placeholder; promoted when a non-null value exists
        AttributeValue.Type.L -> Schema.FieldType.array(Schema.FieldType.STRING)
        AttributeValue.Type.M -> Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.STRING)
        AttributeValue.Type.SS -> Schema.FieldType.array(Schema.FieldType.STRING)
        AttributeValue.Type.NS -> Schema.FieldType.array(Schema.FieldType.STRING)
        AttributeValue.Type.BS -> Schema.FieldType.array(Schema.FieldType.BYTES)
        else -> Schema.FieldType.STRING
    }

    /** When an attribute has mixed types across items, promote to the wider type. */
    private fun promoteType(a: Schema.FieldType, b: Schema.FieldType): Schema.FieldType {
        if (a == b) return a
        // If either is STRING (the NUL placeholder), use the other.
        if (a.typeName == Schema.TypeName.STRING && b.typeName != Schema.TypeName.STRING) return b
        if (b.typeName == Schema.TypeName.STRING && a.typeName != Schema.TypeName.STRING) return a
        // Default to STRING for incompatible types.
        return Schema.FieldType.STRING
    }

    // ---------- DynamoDB -> Beam ----------

    private fun toBeamValue(value: AttributeValue, beamType: Schema.FieldType): Any? {
        return when (beamType.typeName) {
            Schema.TypeName.STRING -> when (value.type()) {
                AttributeValue.Type.S -> value.s()
                AttributeValue.Type.N -> value.n()
                AttributeValue.Type.BOOL -> value.bool().toString()
                AttributeValue.Type.SS -> value.ss().toString()
                AttributeValue.Type.NS -> value.ns().toString()
                AttributeValue.Type.L -> value.l().map { elementToString(it) }.toString()
                AttributeValue.Type.M -> value.m().mapValues { elementToString(it.value) }.toString()
                else -> value.toString()
            }
            Schema.TypeName.DECIMAL -> when (value.type()) {
                AttributeValue.Type.N -> BigDecimal(value.n())
                AttributeValue.Type.S -> BigDecimal(value.s())
                else -> BigDecimal.ZERO
            }
            Schema.TypeName.BOOLEAN -> when (value.type()) {
                AttributeValue.Type.BOOL -> value.bool()
                AttributeValue.Type.S -> value.s().toBooleanStrictOrNull() ?: false
                AttributeValue.Type.N -> value.n().toInt() != 0
                else -> false
            }
            Schema.TypeName.BYTES -> when (value.type()) {
                AttributeValue.Type.B -> value.b().asByteArray()
                AttributeValue.Type.S -> value.s().toByteArray(Charsets.UTF_8)
                else -> ByteArray(0)
            }
            Schema.TypeName.ARRAY -> when (value.type()) {
                AttributeValue.Type.L -> value.l().map { element -> elementToString(element) }
                AttributeValue.Type.SS -> value.ss().toList()
                AttributeValue.Type.NS -> value.ns().toList()
                AttributeValue.Type.BS -> value.bs().map { it.asByteArray() }
                else -> emptyList<Any>()
            }
            Schema.TypeName.MAP -> when (value.type()) {
                AttributeValue.Type.M -> value.m().mapValues { (_, v) -> elementToString(v) }
                else -> emptyMap<String, Any>()
            }
            else -> when (value.type()) {
                AttributeValue.Type.S -> value.s()
                AttributeValue.Type.N -> value.n()
                AttributeValue.Type.BOOL -> value.bool().toString()
                AttributeValue.Type.B -> value.b().asByteArray()
                else -> value.toString()
            }
        }
    }

    /** Extracts a simple string representation from an AttributeValue (used inside lists/maps). */
    private fun elementToString(value: AttributeValue): String = when (value.type()) {
        AttributeValue.Type.S -> value.s()
        AttributeValue.Type.N -> value.n()
        AttributeValue.Type.BOOL -> value.bool().toString()
        AttributeValue.Type.B -> value.b().asByteArray().toString(Charsets.UTF_8)
        AttributeValue.Type.NUL -> "null"
        else -> value.toString()
    }

    // ---------- Beam -> DynamoDB ----------

    private fun toAttributeValue(value: Any?, beamType: Schema.FieldType): AttributeValue {
        val builder = AttributeValue.builder()
        when {
            value == null -> builder.nul(true)
            beamType.typeName == Schema.TypeName.STRING -> builder.s(value.toString())
            beamType.typeName == Schema.TypeName.INT32 -> builder.n((value as Number).toInt().toString())
            beamType.typeName == Schema.TypeName.INT64 -> builder.n((value as Number).toLong().toString())
            beamType.typeName == Schema.TypeName.FLOAT -> builder.n((value as Number).toFloat().toString())
            beamType.typeName == Schema.TypeName.DOUBLE -> builder.n((value as Number).toDouble().toString())
            beamType.typeName == Schema.TypeName.DECIMAL -> builder.n(value.toString())
            beamType.typeName == Schema.TypeName.BOOLEAN -> builder.bool(value as Boolean)
            beamType.typeName == Schema.TypeName.BYTES -> {
                val bytes = when (value) {
                    is ByteArray -> SdkBytes.fromByteArray(value)
                    is SdkBytes -> value
                    is java.nio.ByteBuffer -> SdkBytes.fromByteArray(
                        ByteArray(value.remaining()).also { value.get(it) }
                    )
                    else -> SdkBytes.fromByteArray(value.toString().toByteArray(Charsets.UTF_8))
                }
                builder.b(bytes)
            }
            beamType.typeName == Schema.TypeName.ARRAY -> {
                when (value) {
                    is List<*> -> builder.l(value.map { toAttributeValue(it, Schema.FieldType.STRING) })
                    is Array<*> -> builder.l(value.map { toAttributeValue(it, Schema.FieldType.STRING) })
                    else -> builder.s(value.toString())
                }
            }
            beamType.typeName == Schema.TypeName.MAP -> {
                @Suppress("UNCHECKED_CAST")
                val mapValue = value as? Map<String, Any?> ?: emptyMap()
                builder.m(mapValue.mapValues { (_, v) -> toAttributeValue(v, Schema.FieldType.STRING) })
            }
            else -> builder.s(value.toString())
        }
        return builder.build()
    }
}
