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

    /** csv 表头（字段名列表），写文件时输出。 */
    fun csvHeader(schema: Schema): List<String> = schema.fields.map { it.name }

    /**
     * 把一列字符串字段按 schema 组装成 Row：缺失/空字段填 null，其余按字段类型做类型转换。
     * 用于 `csv`（经 Commons CSV 解析后的字段）与 `xlsx`（经单元格读取后的字段）。
     */
    fun rowFromFields(fields: List<String?>, schema: Schema): Row {
        val row = Row.withSchema(schema)
        schema.fields.forEachIndexed { index, field ->
            row.addValue(coerce(fields.getOrNull(index), field.type))
        }
        return row.build()
    }

    /** 把一行 Row 按 schema 转成字符串字段列表，用于 `csv`(Commons CSV)/`xlsx`(POI 字符串单元格) 写出。 */
    fun rowToFields(row: Row, schema: Schema): List<String?> {
        val result = ArrayList<String?>(schema.fieldCount)
        for (i in 0 until schema.fieldCount) {
            result.add(row.getValue<Any?>(i)?.toString())
        }
        return result
    }

    private fun coerce(raw: String?, type: Schema.FieldType): Any? {
        if (raw == null || raw.isBlank()) return null
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
