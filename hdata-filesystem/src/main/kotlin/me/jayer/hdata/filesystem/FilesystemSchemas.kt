package me.jayer.hdata.filesystem

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row

/**
 * 由 `schema_fields`（`name:type` 列表）构建 Beam schema；未提供时返回单 `content` STRING 列。
 */
object FilesystemSchemas {

    const val CONTENT_FIELD = "content"

    fun build(config: FilesystemReadConfig): Schema =
        if (config.schemaFields.isNotEmpty()) parseSchemaFields(config.schemaFields)
        else Schema.builder().addStringField(CONTENT_FIELD).build()

    fun build(config: FilesystemWriteConfig): Schema =
        if (config.schemaFields.isNotEmpty()) parseSchemaFields(config.schemaFields)
        else Schema.builder().addStringField(CONTENT_FIELD).build()

    private fun parseSchemaFields(fields: List<String>): Schema {
        val builder = Schema.builder()
        fields.forEach { spec ->
            val (name, type) = spec.split(":", limit = 2)
            builder.addNullableField(name.trim(), fieldType(type.trim()))
        }
        return builder.build()
    }

    fun fieldType(type: String): Schema.FieldType = when (type.lowercase()) {
        "string" -> Schema.FieldType.STRING
        "int", "integer" -> Schema.FieldType.INT32
        "long" -> Schema.FieldType.INT64
        "float" -> Schema.FieldType.FLOAT
        "double" -> Schema.FieldType.DOUBLE
        "boolean" -> Schema.FieldType.BOOLEAN
        "short" -> Schema.FieldType.INT16
        "byte" -> Schema.FieldType.BYTE
        else -> throw IllegalArgumentException("不支持的字段类型: $type")
    }

    /** 把一行文本按 `csv` 规则解析成 Row（字段顺序与 schema 一致）。 */
    fun parseCsvLine(line: String, schema: Schema): Row {
        val parts = line.split(",", limit = schema.fieldCount)
        val row = Row.withSchema(schema)
        schema.fields.forEachIndexed { index, field ->
            val raw = parts.getOrNull(index)?.trim() ?: ""
            row.addValue(coerce(raw, field.type))
        }
        return row.build()
    }

    private fun coerce(raw: String, type: Schema.FieldType): Any? {
        if (raw.isEmpty()) return null
        return when (type.typeName) {
            Schema.TypeName.STRING -> raw
            Schema.TypeName.INT32 -> raw.toInt()
            Schema.TypeName.INT64 -> raw.toLong()
            Schema.TypeName.INT16 -> raw.toShort()
            Schema.TypeName.BYTE -> raw.toByte()
            Schema.TypeName.FLOAT -> raw.toFloat()
            Schema.TypeName.DOUBLE -> raw.toDouble()
            Schema.TypeName.BOOLEAN -> raw.toBoolean()
            else -> raw
        }
    }
}
