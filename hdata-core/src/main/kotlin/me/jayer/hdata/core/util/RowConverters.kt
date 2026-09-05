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
 * 配置语法树（[JsonNode]）与 Beam [Row] 之间的转换。
 *
 * 被两处复用：把 pipeline 文件里的字面量变成 `Create` / `AssertEqual` 的数据行，
 * 以及把 config 绑定到 Beam 原生 `SchemaTransformProvider` 要求的配置 [Row] 上。
 *
 * @author wuya
 * @date 2022-08-30
 */
object RowConverters {

    fun toRow(schema: Schema, node: JsonNode, path: String = "$"): Row {
        if (!node.isObject) {
            throw HDataException("$path 期望是对象，实际为: ${node.nodeType}")
        }
        val unknown = node.propertyNames() - schema.fieldNames.toSet()
        if (unknown.isNotEmpty()) {
            throw HDataException("$path 存在未知字段 $unknown，可用字段: ${schema.fieldNames}")
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
                throw HDataException("$path 不可为空")
            }
            return null
        }
        return when (type.typeName) {
            Schema.TypeName.STRING -> {
                requireNodeType(node.isString, path, "字符串", node)
                node.asString()
            }
            Schema.TypeName.BOOLEAN -> {
                requireNodeType(node.isBoolean, path, "布尔值", node)
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
                throw HDataException("$path 无法解析为 DECIMAL: \"${node.asString()}\"", e)
            }
            Schema.TypeName.BYTES -> try {
                requireNodeType(node.isString, path, "base64 字符串", node)
                Base64.getDecoder().decode(node.asString())
            } catch (e: IllegalArgumentException) {
                throw HDataException("$path 不是合法的 base64 字符串", e)
            }
            Schema.TypeName.DATETIME -> try {
                requireNodeType(node.isString, path, "ISO 时间字符串", node)
                DateTime(node.asString(), DateTimeZone.UTC)
            } catch (e: IllegalArgumentException) {
                throw HDataException("$path 无法解析为 DATETIME: \"${node.asString()}\"", e)
            }
            Schema.TypeName.ARRAY, Schema.TypeName.ITERABLE -> {
                if (!node.isArray) throw HDataException("$path 期望是数组，实际为: ${node.nodeType}")
                node.mapIndexed { index, element -> toValue(type.collectionElementType!!, element, "$path[$index]") }
            }

            Schema.TypeName.MAP -> {
                if (!node.isObject) throw HDataException("$path 期望是对象，实际为: ${node.nodeType}")
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
        if (!matches) throw HDataException("$path 期望是$expected，实际为: ${node.nodeType}")
    }

    private fun toLogicalValue(type: Schema.FieldType, node: JsonNode, path: String): Any {
        val identifier = type.logicalType!!.identifier
        requireNodeType(node.isString, path, "ISO 时间字符串", node)
        val text = node.asString()
        return try {
            when (identifier) {
                SqlTypes.DATE.identifier -> LocalDate.parse(text)
                SqlTypes.TIME.identifier -> LocalTime.parse(text)
                SqlTypes.DATETIME.identifier -> LocalDateTime.parse(text)
                SqlTypes.TIMESTAMP.identifier -> Instant.parse(text)
                else -> throw HDataException("$path 的逻辑类型[$identifier] 暂不支持从配置字面量构造")
            }
        } catch (e: java.time.format.DateTimeParseException) {
            throw HDataException("$path 无法解析为 $identifier: \"$text\"", e)
        }
    }

    private fun number(node: JsonNode, path: String): BigDecimal {
        if (!node.isNumber) {
            throw HDataException("$path 期望是数字，实际为: ${node.nodeType}")
        }
        return try {
            BigDecimal(node.asString())
        } catch (e: NumberFormatException) {
            throw HDataException("$path 不是有限的十进制数字: ${node.asString()}", e)
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
        throw HDataException("$path 的值 ${node.asString()} 无法无损转换为 $target", e)
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
            throw HDataException("$path 的值 ${node.asString()} 超出 $target 的有限范围")
        }
        return value
    }

    /**
     * 从若干条字面量记录推断 schema：整数 -> INT64，浮点 -> DOUBLE，其余按字面类型映射；
     * 任一条记录缺字段或为 null 时该字段可空。
     */
    fun inferSchema(elements: List<JsonNode>, path: String = "$"): Schema {
        require(elements.isNotEmpty()) { "无法从空列表推断 schema" }
        elements.forEachIndexed { index, element ->
            if (!element.isObject) {
                throw HDataException("$path[$index] 期望是对象，实际为: ${element.nodeType}")
            }
        }
        val fieldNames = elements.flatMap { it.propertyNames() }.distinct()
        if (fieldNames.isEmpty()) {
            throw HDataException("$path 的记录没有任何字段，无法推断 schema")
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
            // 元素全是 null（或数组本身是空的）时推不出类型，退化成 STRING。
            // 不能直接把过滤后的空列表交给 inferType：空列表上 all{} 恒为 true，
            // 会走进第一条分支推出 BOOLEAN，随后建 Row 时又因为元素类型不可空而报"不可为空"
            val present = values.flatten().filterNot { it.isNull }
            val nullableElements = values.flatten().any { it.isNull }
            val elementType = if (present.isEmpty()) Schema.FieldType.STRING else inferType(present, "$path[]")
            Schema.FieldType.array(
                elementType.withNullable(nullableElements)
            )
        }

        else -> throw HDataException("$path 的取值类型不一致: ${values.map { it.nodeType }.distinct()}")
    }
}

private object TextNodes {
    fun of(value: String): JsonNode = tools.jackson.databind.node.JsonNodeFactory.instance.stringNode(value)
}
