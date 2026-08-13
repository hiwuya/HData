package me.jayer.hdata.filesystem

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import java.io.Serializable

/**
 * `schema_fields` 的解析，以及字符串字段与 Beam [Row] 的互转。
 *
 * @author wuya
 */
object FilesystemSchemas {

    const val CONTENT_FIELD = "content"

    /** `text` 格式的固定 schema：一行一条记录。 */
    val TEXT_SCHEMA: Schema = Schema.builder().addStringField(CONTENT_FIELD).build()

    fun build(schemaFields: List<String>): Schema =
        if (schemaFields.isEmpty()) TEXT_SCHEMA else parseSchemaFields(schemaFields)

    fun build(config: FilesystemReadConfig): Schema = build(config.schemaFields)

    fun build(config: FilesystemWriteConfig): Schema = build(config.schemaFields)

    private fun parseSchemaFields(fields: List<String>): Schema {
        val builder = Schema.builder()
        fields.forEach { spec ->
            val parts = spec.split(":", limit = 2)
            require(parts.size == 2) { "schema_fields 条目格式应为 name:type，收到: $spec" }
            require(parts[0].isNotBlank()) { "schema_fields 条目的字段名不能为空: $spec" }
            builder.addNullableField(parts[0].trim(), fieldType(parts[1].trim()))
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
        else -> throw IllegalArgumentException(
            "schema_fields 不支持的类型: $type，可选 string/int/long/float/double/boolean/short/byte"
        )
    }

    /** csv 表头（字段名列表）。 */
    fun csvHeader(schema: Schema): List<String> = schema.fields.map { it.name }

    /** 把一行 [Row] 转成字符串字段列表，供 csv / xlsx 写出。 */
    fun rowToFields(row: Row, schema: Schema): List<String?> =
        (0 until schema.fieldCount).map { row.getValue<Any?>(it)?.toString() }
}

/**
 * 一批字符串字段到 [Row] 的转换。
 *
 * 解析失败时报错会带上**文件名与行号**：重构前 `raw.toInt()` 抛出的 `NumberFormatException`
 * 只说得出 `For input string: "abc"`，一个几百万行的 CSV 里根本无从查起。
 */
class RecordParser(private val schema: Schema) : Serializable {

    fun parse(fields: List<String?>, source: String, lineNumber: Long): Row {
        val builder = Row.withSchema(schema)
        schema.fields.forEachIndexed { index, field ->
            val raw = fields.getOrNull(index)
            builder.addValue(coerce(raw, field, index, source, lineNumber))
        }
        return builder.build()
    }

    private fun coerce(raw: String?, field: Schema.Field, index: Int, source: String, lineNumber: Long): Any? {
        if (raw == null || raw.isBlank()) {
            return null
        }
        return try {
            when (field.type.typeName) {
                Schema.TypeName.STRING -> raw
                Schema.TypeName.INT32 -> raw.trim().toInt()
                Schema.TypeName.INT64 -> raw.trim().toLong()
                Schema.TypeName.INT16 -> raw.trim().toShort()
                Schema.TypeName.BYTE -> raw.trim().toByte()
                Schema.TypeName.FLOAT -> raw.trim().toFloat()
                Schema.TypeName.DOUBLE -> raw.trim().toDouble()
                // Kotlin 的 String.toBoolean() 把 "1"/"yes"/"Y" 一律当成 false，
                // 静默把数据改错，所以这里只认明确的写法，其余报错
                Schema.TypeName.BOOLEAN -> parseBoolean(raw.trim())
                else -> raw
            }
        } catch (e: Exception) {
            throw IllegalArgumentException(
                "$source 第 $lineNumber 行第 ${index + 1} 列[${field.name}] 无法解析成 ${field.type.typeName}: \"$raw\"",
                e,
            )
        }
    }

    private fun parseBoolean(raw: String): Boolean = when (raw.lowercase()) {
        "true", "1", "yes", "y", "t" -> true
        "false", "0", "no", "n", "f" -> false
        else -> throw IllegalArgumentException("不是布尔值")
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
