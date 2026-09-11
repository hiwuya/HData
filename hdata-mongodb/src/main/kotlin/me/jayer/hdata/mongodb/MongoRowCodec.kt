package me.jayer.hdata.mongodb

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import org.bson.Document
import org.bson.types.Binary
import java.io.Serializable
import java.util.Date
import java.math.BigDecimal

/**
 * Parsing of `schema_fields`, and conversion between Bson [Document] and Beam [Row].
 *
 * Extracted into a single object for two reasons:
 *  - Before the refactor, `documentToRow` / `rowToDocument` re-split and re-validated `schema_fields`
 *    for **every row** — pure waste on the per-row hot path;
 *  - When reading without `schema_fields`, the produced column is named `document`, but the writer
 *    looked for `value`; writing the read data straight back reported a "missing field". Now both
 *    sides use [DOCUMENT_FIELD].
 *
 * @author wuya
 */
class MongoRowCodec private constructor(private val fields: List<MongoField>) : Serializable {

    val schema: Schema = if (fields.isEmpty()) {
        DOCUMENT_SCHEMA
    } else {
        Schema.builder().apply { fields.forEach { addNullableField(it.name, it.type.fieldType) } }.build()
    }

    /** When `schema_fields` is not declared, the whole document is passed as a single JSON column. */
    val documentMode: Boolean get() = fields.isEmpty()

    /** Request only the needed fields so MongoDB transfers less data; in document mode returns null meaning the whole document. */
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
                "Without schema_fields configured, the written row must contain the $DOCUMENT_FIELD (STRING) field " +
                    "(this is the name produced by `ReadFromMongoDb`), existing fields: ${row.schema.fieldNames}"
            }
            val json = row.getString(DOCUMENT_FIELD)
                ?: throw IllegalArgumentException("$DOCUMENT_FIELD field is null and cannot be parsed into a document")
            return Document.parse(json)
        }
        val doc = Document()
        fields.forEach { field ->
            require(row.schema.hasField(field.name)) {
                "Written row is missing the field [${field.name}] declared by schema_fields; existing fields: ${row.schema.fieldNames}"
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

/** Types supported by `schema_fields`. */
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
     * MongoDB is schemaless, so the same field stored as Int or Long across documents is common;
     * therefore numeric types use loose conversion (before the refactor we used `doc.getInteger(name)`,
     * which threw ClassCastException on Long).
     */
    fun fromBson(field: String, value: Any?): Any? {
        if (value == null) {
            return null
        }
        return try {
            when (this) {
                STRING -> value as? String ?: value.toString()
                INT32 -> decimal(field, value).intValueExact()
                INT64 -> decimal(field, value).longValueExact()
                DOUBLE -> decimal(field, value).toDouble().also { require(it.isFinite()) { "Field [$field] is out of the finite range of DOUBLE" } }
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
        } catch (e: ArithmeticException) {
            throw IllegalArgumentException("Value [$value] of field [$field] cannot be losslessly converted to $name", e)
        }
    }

    fun toBson(field: String, value: Any?): Any? {
        if (value == null) {
            return null
        }
        return try {
            when (this) {
                STRING -> value as? String ?: mismatch(field, value)
                INT32 -> decimal(field, value).intValueExact()
                INT64 -> decimal(field, value).longValueExact()
                DOUBLE -> decimal(field, value).toDouble().also { require(it.isFinite()) { "Field [$field] is out of the finite range of DOUBLE" } }
                BOOLEAN -> value as? Boolean ?: mismatch(field, value)
                DATETIME -> when (value) {
                    is org.joda.time.ReadableInstant -> Date(value.millis)
                    is Date -> value
                    else -> mismatch(field, value)
                }
                BYTES -> Binary(value as? ByteArray ?: mismatch(field, value))
            }
        } catch (e: ArithmeticException) {
            throw IllegalArgumentException("Value [$value] of field [$field] cannot be losslessly converted to $name", e)
        }
    }

    private fun decimal(field: String, value: Any): BigDecimal = when (value) {
        is BigDecimal -> value
        is Number -> value.toString().toBigDecimal()
        else -> mismatch(field, value)
    }

    private fun mismatch(field: String, value: Any): Nothing = throw IllegalArgumentException(
        "Field [$field] is declared as $name but actually received ${value.javaClass.simpleName}"
    )

    companion object {
        fun of(name: String): MongoType = entries.firstOrNull { it.name == name.uppercase() }
            ?: throw IllegalArgumentException(
                "Type not supported by schema_fields: $name; available: ${entries.joinToString { it.name }}"
            )
    }
}

fun parseSchemaFields(fields: List<String>): List<MongoField> {
    val parsed = fields.map { spec ->
        val parts = spec.split(":", limit = 2)
        require(parts.size == 2) { "schema_fields entry must be in name:type format, received: $spec" }
        require(parts[0].isNotBlank()) { "schema_fields entry field name must not be empty: $spec" }
        MongoField(parts[0].trim(), MongoType.of(parts[1]))
    }
    require(parsed.map { it.name }.distinct().size == parsed.size) { "schema_fields field names must not be duplicated" }
    return parsed
}
