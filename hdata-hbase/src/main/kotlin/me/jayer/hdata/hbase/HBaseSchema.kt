package me.jayer.hdata.hbase

import org.apache.beam.sdk.schemas.Schema
import org.apache.hadoop.hbase.util.Bytes
import java.math.BigDecimal

/**
 * Parses `schema_fields` and converts between HBase cell bytes and Beam field values.
 *
 * Entries support `qualifier:type`, which uses configured `family`, and `family:qualifier:type`, which explicitly
 * selects a family and permits multiple families in a single operation.
 *
 * @author wuya
 */
data class HBaseColumn(
    val family: String,
    val qualifier: String,
    val type: HBaseType,
) : java.io.Serializable {
    /**
     * The Beam row field name is the qualifier without its family prefix. [buildReadSchema] rejects duplicate
     * qualifiers from distinct families so configuration remains aligned with declared `schema_fields`.
     */
    val fieldName: String get() = qualifier

    val familyBytes: ByteArray get() = Bytes.toBytes(family)
    val qualifierBytes: ByteArray get() = Bytes.toBytes(qualifier)

    companion object {
        fun parse(spec: String, defaultFamily: String): HBaseColumn {
            val parts = spec.split(':').map { it.trim() }
            val (family, qualifier, type) = when (parts.size) {
                1 -> Triple(defaultFamily, parts[0], HBaseType.STRING.name)
                2 -> Triple(defaultFamily, parts[0], parts[1])
                3 -> Triple(parts[0], parts[1], parts[2])
                else -> throw IllegalArgumentException(
                    "schema_fields entries must use qualifier:type or family:qualifier:type; received: $spec"
                )
            }
            require(qualifier.isNotBlank()) { "schema_fields qualifier must not be blank: $spec" }
            require(family.isNotBlank()) { "schema_fields family must not be blank: $spec" }
            return HBaseColumn(family, qualifier, HBaseType.of(type))
        }
    }
}

/** Mapping between byte-encoded HBase values and Beam field types. */
enum class HBaseType(val fieldType: Schema.FieldType, private val width: Int?) {

    STRING(Schema.FieldType.STRING, null),
    INT32(Schema.FieldType.INT32, Bytes.SIZEOF_INT),
    INT64(Schema.FieldType.INT64, Bytes.SIZEOF_LONG),
    DOUBLE(Schema.FieldType.DOUBLE, Bytes.SIZEOF_DOUBLE),
    BOOLEAN(Schema.FieldType.BOOLEAN, Bytes.SIZEOF_BOOLEAN),
    BYTES(Schema.FieldType.BYTES, null),
    ;

    /**
     * Validates fixed-width cells. HBase `Bytes` decoders can silently truncate oversized values, so both short
     * and oversized values are rejected.
     */
    fun decode(column: HBaseColumn, bytes: ByteArray?): Any? {
        if (bytes == null) {
            return null
        }
        if (width != null && bytes.size != width) {
            throw IllegalArgumentException(
                "column[${column.family}:${column.qualifier}] is declared as $name ($width bytes), " +
                    "but its cell contains ${bytes.size} bytes; use STRING or BYTES and parse it explicitly"
            )
        }
        return when (this) {
            STRING -> Bytes.toString(bytes)
            INT32 -> Bytes.toInt(bytes)
            INT64 -> Bytes.toLong(bytes)
            DOUBLE -> Bytes.toDouble(bytes)
            BOOLEAN -> Bytes.toBoolean(bytes)
            BYTES -> bytes
        }
    }

    fun encode(column: HBaseColumn, value: Any?): ByteArray? {
        if (value == null) {
            return null
        }
        return try {
            when (this) {
                STRING -> Bytes.toBytes(value as String)
                INT32 -> Bytes.toBytes(decimal(value).intValueExact())
                INT64 -> Bytes.toBytes(decimal(value).longValueExact())
                DOUBLE -> Bytes.toBytes(decimal(value).toDouble().also { require(it.isFinite()) { "value exceeds DOUBLE's finite range" } })
                BOOLEAN -> Bytes.toBytes(value as Boolean)
                BYTES -> value as ByteArray
            }
        } catch (e: Exception) {
            throw IllegalArgumentException(
                "column[${column.family}:${column.qualifier}] is declared as $name, " +
                    "but the input row value is ${value.javaClass.simpleName}",
                e,
            )
        }
    }

    companion object {
        fun of(name: String): HBaseType = entries.firstOrNull { it.name == name.uppercase() }
            ?: throw IllegalArgumentException(
                "schema_fields does not support type: $name; available: ${entries.joinToString { it.name }}"
            )
    }

    private fun decimal(value: Any): BigDecimal = when (value) {
        is BigDecimal -> value
        is Number -> value.toString().toBigDecimal()
        else -> throw IllegalArgumentException("value is not numeric")
    }
}

/** Row-key representation in Beam rows. Binary row keys require BYTES to prevent corruption. */
enum class RowkeyFormat(val configValue: String, val fieldType: Schema.FieldType) {

    STRING("string", Schema.FieldType.STRING),
    BYTES("bytes", Schema.FieldType.BYTES),
    ;

    fun decode(rowkey: ByteArray): Any = if (this == STRING) Bytes.toString(rowkey) else rowkey

    fun encode(value: Any?): ByteArray {
        checkNotNull(value) { "rowkey must not be null" }
        return when {
            this == BYTES -> value as? ByteArray
                ?: throw IllegalArgumentException("rowkey_format=bytes requires a BYTES rowkey field, but was ${value.javaClass.simpleName}")
            value is String -> Bytes.toBytes(value)
            else -> throw IllegalArgumentException(
                "rowkey_format=string requires a STRING rowkey field, but was ${value.javaClass.simpleName}; " +
                    "use rowkey_format: bytes for binary row keys"
            )
        }
    }

    companion object {
        fun of(value: String): RowkeyFormat = entries.firstOrNull { it.configValue == value }
            ?: throw IllegalArgumentException(
                "invalid rowkey_format: $value; available: ${entries.joinToString { it.configValue }}"
            )
    }
}

fun parseColumns(schemaFields: List<String>?, defaultFamily: String): List<HBaseColumn> =
    schemaFields?.map { HBaseColumn.parse(it, defaultFamily) } ?: emptyList()

/** Schema for read rows: rowkey first, followed by [columns] in order. */
fun buildReadSchema(rowkeyField: String, rowkeyFormat: RowkeyFormat, columns: List<HBaseColumn>): Schema {
    // Same-named qualifiers from different families map to one field name, which Beam does not permit.
    val duplicated = columns.groupingBy { it.fieldName }.eachCount().filterValues { it > 1 }.keys
    require(duplicated.isEmpty()) {
        "schema_fields contains duplicate qualifiers $duplicated, often from different families; " +
            "Beam schemas require unique fields. Keep one family or rename fields with MapToFields after reading"
    }
    require(columns.none { it.fieldName == rowkeyField }) {
        "a schema_fields qualifier conflicts with rowkey_field[$rowkeyField]; change rowkey_field or remove that column"
    }
    val builder = Schema.builder().addField(rowkeyField, rowkeyFormat.fieldType)
    columns.forEach { builder.addNullableField(it.fieldName, it.type.fieldType) }
    return builder.build()
}
