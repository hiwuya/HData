package me.jayer.hdata.mongodb

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import org.bson.Document
import org.bson.types.Binary
import java.io.Serializable
import java.util.Date

/**
 * `schema_fields` 的解析，以及 Bson [Document] 与 Beam [Row] 的互转。
 *
 * 抽成一个对象有两个原因：
 *  - 重构前 `documentToRow` / `rowToDocument` 每处理**一行**就把 `schema_fields` 重新 split + 校验一遍，
 *    这是逐行热路径上的纯浪费；
 *  - 读端不带 `schema_fields` 时产出的列叫 `document`，写端却去找 `value`，
 *    读出来的数据直接写回去会报"缺少字段"。现在两端都用 [DOCUMENT_FIELD]。
 *
 * @author wuya
 */
class MongoRowCodec private constructor(private val fields: List<MongoField>) : Serializable {

    val schema: Schema = if (fields.isEmpty()) {
        DOCUMENT_SCHEMA
    } else {
        Schema.builder().apply { fields.forEach { addNullableField(it.name, it.type.fieldType) } }.build()
    }

    /** 不声明 `schema_fields` 时，整个文档作为一列 JSON 传递。 */
    val documentMode: Boolean get() = fields.isEmpty()

    /** 只请求需要的字段，让 MongoDB 少传一些数据；document 模式下返回 null 表示要整个文档。 */
    fun projection(): Document? =
        if (documentMode) null else Document().apply { fields.forEach { append(it.name, 1) } }

    fun toRow(doc: Document): Row {
        if (documentMode) {
            return Row.withSchema(schema).addValue(doc.toJson()).build()
        }
        val builder = Row.withSchema(schema)
        fields.forEach { builder.addValue(it.type.fromBson(it.name, doc[it.name])) }
        return builder.build()
    }

    fun toDocument(row: Row): Document {
        if (documentMode) {
            require(row.schema.hasField(DOCUMENT_FIELD)) {
                "没有配 schema_fields 时，写入行必须包含 $DOCUMENT_FIELD(STRING) 字段（`ReadFromMongoDb` 产出的就是这个名字），" +
                    "现有字段: ${row.schema.fieldNames}"
            }
            val json = row.getString(DOCUMENT_FIELD)
                ?: throw IllegalArgumentException("$DOCUMENT_FIELD 字段是 null，无法解析成文档")
            return Document.parse(json)
        }
        val doc = Document()
        fields.forEach { field ->
            require(row.schema.hasField(field.name)) {
                "写入行缺少 schema_fields 声明的字段[${field.name}]，现有字段: ${row.schema.fieldNames}"
            }
            doc[field.name] = field.type.toBson(field.name, row.getValue<Any?>(field.name))
        }
        return doc
    }

    companion object {
        private const val serialVersionUID: Long = 1

        const val DOCUMENT_FIELD = "document"

        val DOCUMENT_SCHEMA: Schema = Schema.builder().addNullableStringField(DOCUMENT_FIELD).build()

        fun of(schemaFields: List<String>): MongoRowCodec = MongoRowCodec(parseSchemaFields(schemaFields))
    }
}

data class MongoField(val name: String, val type: MongoType) : Serializable {
    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/** `schema_fields` 支持的类型。 */
enum class MongoType(val fieldType: Schema.FieldType) {

    STRING(Schema.FieldType.STRING),
    INT32(Schema.FieldType.INT32),
    INT64(Schema.FieldType.INT64),
    DOUBLE(Schema.FieldType.DOUBLE),
    BOOLEAN(Schema.FieldType.BOOLEAN),
    DATETIME(Schema.FieldType.DATETIME),
    BYTES(Schema.FieldType.BYTES),
    ;

    /**
     * MongoDB 是无 schema 的，同一个字段在不同文档里存成 Int 或 Long 都很常见，
     * 所以数值类型之间做宽松转换（重构前用 `doc.getInteger(name)`，遇到 Long 直接 ClassCastException）。
     */
    fun fromBson(field: String, value: Any?): Any? {
        if (value == null) {
            return null
        }
        return when (this) {
            STRING -> value as? String ?: value.toString()
            INT32 -> number(field, value).toInt()
            INT64 -> number(field, value).toLong()
            DOUBLE -> number(field, value).toDouble()
            BOOLEAN -> value as? Boolean ?: mismatch(field, value)
            DATETIME -> when (value) {
                is Date -> org.joda.time.Instant(value.time)
                is java.time.Instant -> org.joda.time.Instant(value.toEpochMilli())
                else -> mismatch(field, value)
            }
            BYTES -> when (value) {
                is Binary -> value.data
                is ByteArray -> value
                else -> mismatch(field, value)
            }
        }
    }

    fun toBson(field: String, value: Any?): Any? {
        if (value == null) {
            return null
        }
        return when (this) {
            STRING, INT32, INT64, DOUBLE, BOOLEAN -> value
            DATETIME -> when (value) {
                is org.joda.time.ReadableInstant -> Date(value.millis)
                is Date -> value
                else -> mismatch(field, value)
            }
            BYTES -> Binary(value as? ByteArray ?: mismatch(field, value))
        }
    }

    private fun number(field: String, value: Any): Number =
        value as? Number ?: mismatch(field, value)

    private fun mismatch(field: String, value: Any): Nothing = throw IllegalArgumentException(
        "字段[$field] 声明为 $name，实际拿到的是 ${value.javaClass.simpleName}"
    )

    companion object {
        fun of(name: String): MongoType = entries.firstOrNull { it.name == name.uppercase() }
            ?: throw IllegalArgumentException(
                "schema_fields 不支持的类型: $name，可选 ${entries.joinToString { it.name }}"
            )
    }
}

fun parseSchemaFields(fields: List<String>): List<MongoField> = fields.map { spec ->
    val parts = spec.split(":", limit = 2)
    require(parts.size == 2) { "schema_fields 条目格式应为 name:type，收到: $spec" }
    require(parts[0].isNotBlank()) { "schema_fields 条目的字段名不能为空: $spec" }
    MongoField(parts[0].trim(), MongoType.of(parts[1]))
}
