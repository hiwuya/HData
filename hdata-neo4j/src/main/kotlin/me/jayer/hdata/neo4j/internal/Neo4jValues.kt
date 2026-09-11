package me.jayer.hdata.neo4j.internal

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import org.neo4j.driver.Record
import org.neo4j.driver.Value
import java.math.BigDecimal

/**
 * Parses `name:TYPE` declarations into field names and Beam types.
 *
 * @author wuya
 */
fun parseSchemaFields(fields: List<String>): List<Pair<String, Schema.FieldType>> =
    fields.map { spec ->
        val parts = spec.split(":", limit = 2)
        val name = parts[0].trim()
        require(name.isNotBlank()) { "schema_fields field name must not be blank: $spec" }
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
    else -> throw IllegalArgumentException("unsupported Neo4j field type: $type")
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
            Schema.TypeName.DOUBLE -> decimal(raw).toDouble().also { require(it.isFinite()) { "value exceeds the finite DOUBLE range" } }
            Schema.TypeName.FLOAT -> decimal(raw).toFloat().also { require(it.isFinite()) { "value exceeds the finite FLOAT range" } }
            Schema.TypeName.BOOLEAN -> raw as Boolean
            Schema.TypeName.BYTES -> raw as? ByteArray ?: throw IllegalArgumentException("value is not a ByteArray")
            else -> throw IllegalArgumentException("unsupported Beam type ${type.typeName}")
        }
    } catch (e: Exception) {
        throw IllegalArgumentException("Neo4j value [$raw] cannot be converted to ${type.typeName}", e)
    }
}

private fun decimal(value: Any): BigDecimal = when (value) {
    is BigDecimal -> value
    is Number -> value.toString().toBigDecimal()
    else -> throw IllegalArgumentException("value is not numeric")
}

fun recordToRow(record: Record, schemaFields: List<Pair<String, Schema.FieldType>>, schema: Schema): Row {
    val builder = Row.withSchema(schema)
    schemaFields.forEach { (name, type) ->
        val value: Value = record.get(name)
        builder.addValue(convertToRowValue(value.asObject(), type))
    }
    return builder.build()
}

/** Cypher lexical state; `$` is a parameter only in ordinary code, not literals, identifiers, or comments. */
private enum class CypherLexState { CODE, SINGLE_QUOTE, DOUBLE_QUOTE, BACKTICK, LINE_COMMENT, BLOCK_COMMENT }

fun extractCypherParams(statement: String): Set<String> {
    val names = linkedSetOf<String>()
    var state = CypherLexState.CODE
    var index = 0
    while (index < statement.length) {
        val current = statement[index]
        val next = statement.getOrNull(index + 1)
        when (state) {
            CypherLexState.CODE -> when {
                current == '\'' -> state = CypherLexState.SINGLE_QUOTE
                current == '"' -> state = CypherLexState.DOUBLE_QUOTE
                current == '`' -> state = CypherLexState.BACKTICK
                current == '/' && next == '/' -> {
                    state = CypherLexState.LINE_COMMENT
                    index++
                }
                current == '/' && next == '*' -> {
                    state = CypherLexState.BLOCK_COMMENT
                    index++
                }
                current == '$' && next != null && (next == '_' || next.isLetter()) -> {
                    var end = index + 2
                    while (end < statement.length) {
                        val char = statement[end]
                        if (char != '_' && !char.isLetterOrDigit()) break
                        end++
                    }
                    names += statement.substring(index + 1, end)
                    index = end - 1
                }
            }
            CypherLexState.SINGLE_QUOTE -> {
                if (current == '\\' && next != null) index++
                else if (current == '\'' && next == '\'') index++
                else if (current == '\'') state = CypherLexState.CODE
            }
            CypherLexState.DOUBLE_QUOTE -> {
                if (current == '\\' && next != null) index++
                else if (current == '"' && next == '"') index++
                else if (current == '"') state = CypherLexState.CODE
            }
            CypherLexState.BACKTICK -> {
                if (current == '`' && next == '`') index++
                else if (current == '`') state = CypherLexState.CODE
            }
            CypherLexState.LINE_COMMENT -> if (current == '\n' || current == '\r') state = CypherLexState.CODE
            CypherLexState.BLOCK_COMMENT -> if (current == '*' && next == '/') {
                state = CypherLexState.CODE
                index++
            }
        }
        index++
    }
    return names
}

/**
 * Builds execution parameters from a Cypher statement and a row.
 */
fun buildParams(row: Row, statement: String, mapping: Map<String, String>?): Map<String, Any?> {
    val names = extractCypherParams(statement)
    val effective = mapping ?: names.associateWith { it }
    return effective.mapValues { (_, rowField) ->
        require(row.schema.hasField(rowField)) { "row written to Neo4j is missing field $rowField; available fields: ${row.schema.fieldNames}" }
        row.getValue(rowField)
    }
}
