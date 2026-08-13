package me.jayer.hdata.mongodb

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import org.bson.Document
import org.bson.types.Binary
import java.util.Date

/**
 * 解析 `schema_fields`（`name:type` 列表），并做 Bson <-> Beam Row 的双向转换。
 *
 * 类型 ∈ STRING / INT32 / INT64 / DOUBLE / BOOLEAN / DATETIME / BYTES。
 */
data class FieldDef(val name: String, val type: String)

private val DOCUMENT_SCHEMA: Schema = Schema.builder()
    .addNullableField("document", Schema.FieldType.STRING)
    .build()

fun isDocumentSchema(schema: Schema): Boolean = schema == DOCUMENT_SCHEMA

fun parseSchemaFields(fields: List<String>): List<FieldDef> =
    fields.map { spec ->
        val parts = spec.split(":", limit = 2)
        require(parts.size == 2) { "schema_fields 条目格式应为 name:type，收到: $spec" }
        val type = parts[1].uppercase()
        require(type in setOf("STRING", "INT32", "INT64", "DOUBLE", "BOOLEAN", "DATETIME", "BYTES")) {
            "schema_fields 不支持的类型: $type"
        }
        FieldDef(parts[0], type)
    }

fun buildSchema(fields: List<String>): Schema {
    if (fields.isEmpty()) {
        return DOCUMENT_SCHEMA
    }
    val builder = Schema.builder()
    parseSchemaFields(fields).forEach { (name, type) ->
        builder.addNullableField(name, fieldType(type))
    }
    return builder.build()
}

private fun fieldType(type: String): Schema.FieldType = when (type) {
    "STRING" -> Schema.FieldType.STRING
    "INT32" -> Schema.FieldType.INT32
    "INT64" -> Schema.FieldType.INT64
    "DOUBLE" -> Schema.FieldType.DOUBLE
    "BOOLEAN" -> Schema.FieldType.BOOLEAN
    "DATETIME" -> Schema.FieldType.DATETIME
    "BYTES" -> Schema.FieldType.BYTES
    else -> throw IllegalArgumentException("不支持的类型: $type")
}

fun documentToRow(doc: Document, schema: Schema, fields: List<String>): Row {
    if (fields.isEmpty()) {
        return Row.withSchema(schema).addValue(doc.toJson()).build()
    }
    val defs = parseSchemaFields(fields)
    val rowBuilder = Row.withSchema(schema)
    defs.forEach { (name, type) ->
        rowBuilder.addValue(toFieldValue(doc, name, type))
    }
    return rowBuilder.build()
}

private fun toFieldValue(doc: Document, name: String, type: String): Any? = when (type) {
    "STRING" -> doc.getString(name)
    "INT32" -> doc.getInteger(name)
    "INT64" -> doc.getLong(name)
    "DOUBLE" -> doc.getDouble(name)
    "BOOLEAN" -> doc.getBoolean(name)
    "DATETIME" -> doc.getDate(name)?.let { Date(it.time) }?.let { org.joda.time.Instant(it.time) }
    "BYTES" -> doc.get(name, Binary::class.java)?.data
    else -> null
}

fun rowToDocument(row: Row, fields: List<String>): Document {
    if (fields.isEmpty()) {
        val json = row.getString("value")
            ?: throw IllegalStateException("无 schema_fields 时，写入行必须包含 value(STRING) 字段")
        return Document.parse(json)
    }
    val doc = Document()
    parseSchemaFields(fields).forEach { (name, type) ->
        doc[name] = fromFieldValue(row, name, type)
    }
    return doc
}

private fun fromFieldValue(row: Row, name: String, type: String): Any? = when (type) {
    "STRING" -> row.getString(name)
    "INT32" -> row.getInt32(name)
    "INT64" -> row.getInt64(name)
    "DOUBLE" -> row.getDouble(name)
    "BOOLEAN" -> row.getBoolean(name)
    "DATETIME" -> row.getDateTime(name)?.let { Date(it.millis) }
    "BYTES" -> row.getBytes(name)?.let { Binary(it) }
    else -> null
}
