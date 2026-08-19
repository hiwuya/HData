package me.jayer.hdata.neo4j.internal

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import org.neo4j.driver.Record
import org.neo4j.driver.Value
import java.math.BigDecimal

/**
 * 把 `name:TYPE` 形式的字段声明解析成 `(字段名, Beam 类型)` 列表。
 *
 * @author wuya
 */
fun parseSchemaFields(fields: List<String>): List<Pair<String, Schema.FieldType>> =
    fields.map { spec ->
        val parts = spec.split(":", limit = 2)
        val name = parts[0].trim()
        require(name.isNotBlank()) { "schema_fields 字段名不能为空: $spec" }
        val type = parts.getOrNull(1)?.trim()?.takeIf { it.isNotBlank() } ?: "STRING"
        name to fieldTypeOf(type)
    }

fun fieldTypeOf(type: String): Schema.FieldType = when (type.uppercase()) {
    "STRING" -> Schema.FieldType.STRING
    "INT64", "BIGINT", "LONG" -> Schema.FieldType.INT64
    "INT32", "INT" -> Schema.FieldType.INT32
    "INT16", "SMALLINT", "SHORT" -> Schema.FieldType.INT16
    "INT8", "TINYINT", "BYTE" -> Schema.FieldType.BYTE
    "FLOAT64", "DOUBLE" -> Schema.FieldType.DOUBLE
    "FLOAT32", "FLOAT", "REAL" -> Schema.FieldType.FLOAT
    "BOOLEAN", "BOOL" -> Schema.FieldType.BOOLEAN
    "BYTES", "BINARY", "BLOB" -> Schema.FieldType.BYTES
    else -> throw IllegalArgumentException("不支持的 Neo4j 字段类型: $type")
}

fun convertToRowValue(raw: Any?, type: Schema.FieldType): Any? {
    if (raw == null) return null
    return try {
        when (type.typeName) {
            Schema.TypeName.STRING -> raw.toString()
            Schema.TypeName.INT64 -> decimal(raw).longValueExact()
            Schema.TypeName.INT32 -> decimal(raw).intValueExact()
            Schema.TypeName.INT16 -> decimal(raw).shortValueExact()
            Schema.TypeName.BYTE -> decimal(raw).byteValueExact()
            Schema.TypeName.DOUBLE -> decimal(raw).toDouble().also { require(it.isFinite()) { "超出 DOUBLE 有限范围" } }
            Schema.TypeName.FLOAT -> decimal(raw).toFloat().also { require(it.isFinite()) { "超出 FLOAT 有限范围" } }
            Schema.TypeName.BOOLEAN -> raw as Boolean
            Schema.TypeName.BYTES -> raw as? ByteArray ?: throw IllegalArgumentException("不是 ByteArray")
            else -> throw IllegalArgumentException("不支持的 Beam 类型 ${type.typeName}")
        }
    } catch (e: Exception) {
        throw IllegalArgumentException("Neo4j 值[$raw]无法转换为 ${type.typeName}", e)
    }
}

private fun decimal(value: Any): BigDecimal = when (value) {
    is BigDecimal -> value
    is Number -> value.toString().toBigDecimal()
    else -> throw IllegalArgumentException("不是数字")
}

fun recordToRow(record: Record, schemaFields: List<Pair<String, Schema.FieldType>>, schema: Schema): Row {
    val builder = Row.withSchema(schema)
    schemaFields.forEach { (name, type) ->
        val value: Value = record.get(name)
        builder.addValue(convertToRowValue(value.asObject(), type))
    }
    return builder.build()
}

private val PARAM_REGEX = Regex("\\\$(\\w+)")

fun extractCypherParams(statement: String): Set<String> =
    PARAM_REGEX.findAll(statement).map { it.groupValues[1] }.toSet()

/**
 * 由 Cypher 语句与行构造执行参数。
 *
 * - 显式映射 `cypher参数名 -> 行字段名`；
 * - 否则自动从 `statement` 提取 `$xxx`，按同名绑定行字段。
 */
fun buildParams(row: Row, statement: String, mapping: Map<String, String>?): Map<String, Any?> {
    val names = extractCypherParams(statement)
    val effective = mapping ?: names.associateWith { it }
    return effective.mapValues { (_, rowField) ->
        require(row.schema.hasField(rowField)) { "写 Neo4j 的行缺少字段 $rowField，现有字段: ${row.schema.fieldNames}" }
        row.getValue(rowField)
    }
}
