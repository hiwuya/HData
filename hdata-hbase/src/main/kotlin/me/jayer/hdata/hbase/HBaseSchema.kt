package me.jayer.hdata.hbase

import org.apache.beam.sdk.schemas.Schema
import org.apache.hadoop.hbase.util.Bytes

/**
 * 配置里 `schema_fields` 单条 `name:type` 的解析结果。
 *
 * `type` ∈ STRING / INT32 / INT64 / DOUBLE / BOOLEAN / BYTES。
 */
data class FieldDef(val name: String, val type: String) {
    init {
        require(type.uppercase() in SUPPORTED_TYPES) { "不支持的字段类型: $type（支持 $SUPPORTED_TYPES）" }
    }

    val normalizedType: String get() = type.uppercase()

    companion object {
        private val SUPPORTED_TYPES = setOf("STRING", "INT32", "INT64", "DOUBLE", "BOOLEAN", "BYTES")

        /** 解析 `"name:type"` 形式的字符串，缺省类型按 STRING 处理。 */
        fun parse(spec: String): FieldDef {
            val idx = spec.indexOf(':')
            return if (idx < 0) {
                FieldDef(spec.trim(), "STRING")
            } else {
                FieldDef(spec.substring(0, idx).trim(), spec.substring(idx + 1).trim())
            }
        }
    }
}

/** 把 `schema_fields` 列表解析成 [FieldDef]。 */
fun parseSchemaFields(schemaFields: List<String>?): List<FieldDef> =
    schemaFields?.map { FieldDef.parse(it) } ?: emptyList()

/** 根据 rowkey 字段名 + 列字段定义构造读出的 Beam Schema（rowkey 恒为 STRING，排在最前）。 */
fun buildReadSchema(rowkeyField: String, fields: List<FieldDef>): Schema {
    val builder = Schema.builder().addStringField(rowkeyField)
    fields.forEach { builder.addNullableField(it.name, fieldType(it.normalizedType)) }
    return builder.build()
}

private fun fieldType(type: String): Schema.FieldType = when (type) {
    "STRING" -> Schema.FieldType.STRING
    "INT32" -> Schema.FieldType.INT32
    "INT64" -> Schema.FieldType.INT64
    "DOUBLE" -> Schema.FieldType.DOUBLE
    "BOOLEAN" -> Schema.FieldType.BOOLEAN
    "BYTES" -> Schema.FieldType.BYTES
    else -> throw IllegalStateException("不支持的字段类型: $type")
}

/** 把 HBase 单元格的字节数组按类型还原成 Beam 行字段值。 */
fun decodeCell(type: String, bytes: ByteArray?): Any? {
    if (bytes == null) return null
    return when (type) {
        "STRING" -> Bytes.toString(bytes)
        "INT32" -> Bytes.toInt(bytes)
        "INT64" -> Bytes.toLong(bytes)
        "DOUBLE" -> Bytes.toDouble(bytes)
        "BOOLEAN" -> Bytes.toBoolean(bytes)
        "BYTES" -> bytes
        else -> throw IllegalStateException("不支持的字段类型: $type")
    }
}

/** 把输入行里的某个字段值按类型编码成写入 HBase 的字节数组。 */
fun encodeCell(type: String, value: Any?): ByteArray? {
    if (value == null) return null
    return when (type) {
        "STRING" -> Bytes.toBytes(value as String)
        "INT32" -> Bytes.toBytes((value as Number).toInt())
        "INT64" -> Bytes.toBytes((value as Number).toLong())
        "DOUBLE" -> Bytes.toBytes((value as Number).toDouble())
        "BOOLEAN" -> Bytes.toBytes(value as Boolean)
        "BYTES" -> value as ByteArray
        else -> throw IllegalStateException("不支持的字段类型: $type")
    }
}
