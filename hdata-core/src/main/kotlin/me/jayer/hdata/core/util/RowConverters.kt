package me.jayer.hdata.core.util

import me.jayer.hdata.core.exception.HDataException
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes
import org.apache.beam.sdk.values.Row
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import tools.jackson.databind.JsonNode
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.Base64

/**
 * Conversion between the config syntax tree ([JsonNode]) and Beam [Row].
 *
 * Reused in two places: turning literals in the pipeline file into data rows for `Create` /
 * `AssertEqual`, and binding config onto the config [Row] required by Beam's native
 * `SchemaTransformProvider`.
 *
 * @author wuya
 * @date 2022-08-30
 */
object RowConverters {

    fun toRow(schema: Schema, node: JsonNode, path: String = "$"): Row {
        if (!node.isObject) {
            throw HDataException("$path expects an object, but got: ${node.nodeType}")
        }
        val unknown = node.propertyNames() - schema.fieldNames.toSet()
        if (unknown.isNotEmpty()) {
            throw HDataException("$path has unknown fields $unknown; available fields: ${schema.fieldNames}")
        }
        val builder = Row.withSchema(schema)
        for (field in schema.fields) {
            builder.addValue(toValue(field.type, node.get(field.name), "$path.${field.name}"))
        }
        return builder.build()
    }

    private fun toValue(type: Schema.FieldType, node: JsonNode?, path: String): Any? {
        if (node == null || node.isNull || node.isMissingNode) {
            if (!type.nullable) {
                throw HDataException("$path must not be null")
            }
            return null
        }
        return when (type.typeName) {
            Schema.TypeName.STRING -> {
                requireNodeType(node.isString, path, "string", node)
                node.asString()
            }
            Schema.TypeName.BOOLEAN -> {
                requireNodeType(node.isBoolean, path, "boolean", node)
                node.booleanValue()
            }
            Schema.TypeName.BYTE -> exactNumber(node, path, "BYTE", BigDecimal::byteValueExact)
            Schema.TypeName.INT16 -> exactNumber(node, path, "INT16", BigDecimal::shortValueExact)
            Schema.TypeName.INT32 -> exactNumber(node, path, "INT32", BigDecimal::intValueExact)
            Schema.TypeName.INT64 -> exactNumber(node, path, "INT64", BigDecimal::longValueExact)
            Schema.TypeName.FLOAT -> finiteNumber(node, path, "FLOAT") { it.toFloat() }
            Schema.TypeName.DOUBLE -> finiteNumber(node, path, "DOUBLE") { it.toDouble() }
            Schema.TypeName.DECIMAL -> try {
                BigDecimal(node.asString())
            } catch (e: NumberFormatException) {
                throw HDataException("$path cannot parse as DECIMAL: \"${node.asString()}\"", e)
            }
            Schema.TypeName.BYTES -> try {
                requireNodeType(node.isString, path, "base64 string", node)
                Base64.getDecoder().decode(node.asString())
            } catch (e: IllegalArgumentException) {
                throw HDataException("$path is not a valid base64 string", e)
            }
            Schema.TypeName.DATETIME -> try {
                requireNodeType(node.isString, path, "ISO datetime string", node)
                DateTime(node.asString(), DateTimeZone.UTC)
            } catch (e: IllegalArgumentException) {
                throw HDataException("$path cannot parse as DATETIME: \"${node.asString()}\"", e)
            }
            Schema.TypeName.ARRAY, Schema.TypeName.ITERABLE -> {
                if (!node.isArray) throw HDataException("$path expects an array, but got: ${node.nodeType}")
                node.mapIndexed { index, element -> toValue(type.collectionElementType!!, element, "$path[$index]") }
            }

            Schema.TypeName.MAP -> {
                if (!node.isObject) throw HDataException("$path expects an object, but got: ${node.nodeType}")
                node.properties().associate { (key, value) ->
                    toValue(type.mapKeyType!!, TextNodes.of(key), "$path.$key") to
                        toValue(type.mapValueType!!, value, "$path.$key")
                }
            }

            Schema.TypeName.ROW -> toRow(type.rowSchema!!, node, path)
            Schema.TypeName.LOGICAL_TYPE -> toLogicalValue(type, node, path)
        }
    }

    private fun requireNodeType(matches: Boolean, path: String, expected: String, node: JsonNode) {
        if (!matches) throw HDataException("$path expects $expected, but got: ${node.nodeType}")
    }

    private fun toLogicalValue(type: Schema.FieldType, node: JsonNode, path: String): Any {
        val identifier = type.logicalType!!.identifier
        requireNodeType(node.isString, path, "ISO datetime string", node)
        val text = node.asString()
        return try {
            when (identifier) {
                SqlTypes.DATE.identifier -> LocalDate.parse(text)
                SqlTypes.TIME.identifier -> LocalTime.parse(text)
                SqlTypes.DATETIME.identifier -> LocalDateTime.parse(text)
                SqlTypes.TIMESTAMP.identifier -> Instant.parse(text)
                else -> throw HDataException("$path's logical type [$identifier] does not yet support construction from config literals")
            }
        } catch (e: java.time.format.DateTimeParseException) {
            throw HDataException("$path cannot parse as $identifier: \"$text\"", e)
        }
    }

    private fun number(node: JsonNode, path: String): BigDecimal {
        if (!node.isNumber) {
            throw HDataException("$path expects a number, but got: ${node.nodeType}")
        }
        return try {
            BigDecimal(node.asString())
        } catch (e: NumberFormatException) {
            throw HDataException("$path is not a finite decimal number: ${node.asString()}", e)
        }
    }

    private inline fun <T> exactNumber(
        node: JsonNode,
        path: String,
        target: String,
        convert: (BigDecimal) -> T,
    ): T = try {
        convert(number(node, path))
    } catch (e: ArithmeticException) {
        throw HDataException("$path's value ${node.asString()} cannot be losslessly converted to $target", e)
    }

    private inline fun <T : Number> finiteNumber(
        node: JsonNode,
        path: String,
        target: String,
        convert: (BigDecimal) -> T,
    ): T {
        val value = convert(number(node, path))
        val finite = when (value) {
            is Float -> value.isFinite()
            is Double -> value.isFinite()
            else -> true
        }
        if (!finite) {
            throw HDataException("$path's value ${node.asString()} is out of $target's finite range")
        }
        return value
    }

    /**
     * Infer the schema from a list of literal records: integer -> INT64, float -> DOUBLE,
     * the rest map to their literal types; any record missing a field or null makes that field nullable.
     */
    fun inferSchema(elements: List<JsonNode>, path: String = "$"): Schema {
        require(elements.isNotEmpty()) { "cannot infer schema from an empty list" }
        elements.forEachIndexed { index, element ->
            if (!element.isObject) {
                throw HDataException("$path[$index] expects an object, but got: ${element.nodeType}")
            }
        }
        val fieldNames = elements.flatMap { it.propertyNames() }.distinct()
        if (fieldNames.isEmpty()) {
            throw HDataException("$path's record has no fields, cannot infer schema")
        }
        val builder = Schema.builder()
        for (name in fieldNames) {
            val values = elements.map { it.get(name) }
            val nullable = values.any { it == null || it.isNull }
            val present = values.filterNotNull().filterNot { it.isNull }
            val type = if (present.isEmpty()) {
                Schema.FieldType.STRING
            } else {
                inferType(present, "$path.$name")
            }
            builder.addField(Schema.Field.of(name, type).withNullable(nullable))
        }
        return builder.build()
    }

    private fun inferType(values: List<JsonNode>, path: String): Schema.FieldType = when {
        values.all { it.isBoolean } -> Schema.FieldType.BOOLEAN
        values.all { it.isIntegralNumber } -> Schema.FieldType.INT64
        values.all { it.isNumber } -> Schema.FieldType.DOUBLE
        values.all { it.isString } -> Schema.FieldType.STRING
        values.all { it.isObject } -> Schema.FieldType.row(inferSchema(values, path))
        values.all { it.isArray } -> {
            // When every element is null (or the array itself is empty) the type cannot be inferred, so fall back to STRING.
            // Cannot hand the filtered empty list straight to inferType: on an empty list all{} is always true,
            // which would hit the first branch and infer BOOLEAN, then fail with "must not be null" when building the Row because the element type is not nullable.
            val present = values.flatten().filterNot { it.isNull }
            val nullableElements = values.flatten().any { it.isNull }
            val elementType = if (present.isEmpty()) Schema.FieldType.STRING else inferType(present, "$path[]")
            Schema.FieldType.array(
                elementType.withNullable(nullableElements)
            )
        }

        else -> throw HDataException("$path has inconsistent value types: ${values.map { it.nodeType }.distinct()}")
    }
}

private object TextNodes {
    fun of(value: String): JsonNode = tools.jackson.databind.node.JsonNodeFactory.instance.stringNode(value)
}
