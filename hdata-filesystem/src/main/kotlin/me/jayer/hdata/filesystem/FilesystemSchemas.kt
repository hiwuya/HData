package me.jayer.hdata.filesystem

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import java.io.Serializable
import java.io.StringWriter
import org.apache.commons.csv.CSVFormat

/**
 * Parsing of `schema_fields`, and conversion between string fields and Beam [Row].
 *
 * @author wuya
 */
object FilesystemSchemas {

    const val CONTENT_FIELD = "content"

    /** Fixed schema for the `text` format: one record per line. */
    val TEXT_SCHEMA: Schema = Schema.builder().addStringField(CONTENT_FIELD).build()

    fun build(schemaFields: List<String>): Schema =
        if (schemaFields.isEmpty()) TEXT_SCHEMA else parseSchemaFields(schemaFields)

    fun build(config: FilesystemReadConfig): Schema = build(config.schemaFields)

    fun build(config: FilesystemWriteConfig): Schema = build(config.schemaFields)

    private fun parseSchemaFields(fields: List<String>): Schema {
        val names = fields.map { it.substringBefore(':').trim() }
        require(names.size == names.distinct().size) { "schema_fields has duplicate field names: $names" }
        val builder = Schema.builder()
        fields.forEach { spec ->
            val parts = spec.split(":", limit = 2)
            require(parts.size == 2) { "schema_fields entries must be in name:type format, got: $spec" }
            require(parts[0].isNotBlank()) { "schema_fields entry must have a non-empty field name: $spec" }
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
            "schema_fields unsupported type: $type, expected one of string/int/long/float/double/boolean/short/byte"
        )
    }

    /** CSV header (list of field names). */
    fun csvHeader(schema: Schema): List<String> = schema.fields.map { it.name }

    /** Convert a [Row] into a list of string fields, for csv / xlsx output. */
    fun rowToFields(row: Row, schema: Schema): List<String?> =
        schema.fields.map { field ->
            require(row.schema.hasField(field.name)) {
                "Input row is missing field [${field.name}] declared in schema_fields; existing fields: ${row.schema.fieldNames}"
            }
            row.getValue<Any?>(field.name)?.toString()
        }

    fun csvRecord(fields: List<Any?>, delimiter: Char, quote: Char): String {
        val writer = StringWriter()
        CSVFormat.DEFAULT.builder().setDelimiter(delimiter).setQuote(quote).get()
            .print(writer).use { it.printRecord(fields) }
        return writer.toString().trimEnd('\r', '\n')
    }
}

/**
 * Conversion of a batch of string fields to [Row].
 *
 * On parse failure the error carries the **file name and line number**: before the refactor the
 * `NumberFormatException` thrown by `raw.toInt()` could only say `For input string: "abc"`, which is
 * impossible to trace in a CSV of millions of rows.
 */
class RecordParser(private val schema: Schema) : Serializable {

    fun parse(fields: List<String?>, source: String, lineNumber: Long): Row {
        require(fields.size <= schema.fieldCount) {
            "$source line $lineNumber has ${fields.size} columns, exceeding the ${schema.fieldCount} declared in schema_fields"
        }
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
                // Kotlin's String.toBoolean() treats "1"/"yes"/"Y" as false, silently corrupting the
                // data, so here we only accept explicit forms and error on the rest
                Schema.TypeName.BOOLEAN -> parseBoolean(raw.trim())
                else -> raw
            }
        } catch (e: Exception) {
            throw IllegalArgumentException(
                "$source line $lineNumber column ${index + 1} [${field.name}] cannot be parsed as ${field.type.typeName}: \"$raw\"",
                e,
            )
        }
    }

    private fun parseBoolean(raw: String): Boolean = when (raw.lowercase()) {
        "true", "1", "yes", "y", "t" -> true
        "false", "0", "no", "n", "f" -> false
        else -> throw IllegalArgumentException("not a boolean value")
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
