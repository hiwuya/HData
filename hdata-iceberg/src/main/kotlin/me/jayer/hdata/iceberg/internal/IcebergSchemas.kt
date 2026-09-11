package me.jayer.hdata.iceberg.internal

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.Record
import org.apache.iceberg.types.Types
import java.nio.ByteBuffer
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.math.BigDecimal

/**
 * Field type mapping between Iceberg and Beam: `name:TYPE` list -> Beam schema / Iceberg schema, plus row conversion
 * in both directions.
 *
 * Supports Iceberg's primitive types (largely aligned with Beam); nested / list / map are not yet supported and can
 * be added as scenarios require.
 *
 * @author wuya
 */
fun parseSchemaFields(fields: List<String>): List<Pair<String, Schema.FieldType>> =
    fields.map { spec ->
        val parts = spec.split(":", limit = 2)
        val name = parts[0].trim()
        require(name.isNotBlank()) { "schema_fields field name must not be empty: $spec" }
        val type = parts.getOrNull(1)?.trim()?.takeIf { it.isNotBlank() } ?: "STRING"
        name to fieldTypeOf(type)
    }

fun fieldTypeOf(type: String): Schema.FieldType = when (type.uppercase()) {
    "STRING" -> Schema.FieldType.STRING
    "INT64", "BIGINT", "LONG" -> Schema.FieldType.INT64
    "INT32", "INT" -> Schema.FieldType.INT32
    "FLOAT64", "DOUBLE" -> Schema.FieldType.DOUBLE
    "FLOAT32", "FLOAT", "REAL" -> Schema.FieldType.FLOAT
    "BOOLEAN", "BOOL" -> Schema.FieldType.BOOLEAN
    "BYTES", "BINARY", "BLOB" -> Schema.FieldType.BYTES
    "DATETIME", "TIMESTAMP" -> Schema.FieldType.DATETIME
    else -> throw IllegalArgumentException("Unsupported Iceberg field type: $type")
}

fun toIcebergType(type: Schema.FieldType): org.apache.iceberg.types.Type = when (type.typeName) {
    Schema.TypeName.STRING -> Types.StringType.get()
    Schema.TypeName.INT64 -> Types.LongType.get()
    Schema.TypeName.INT32 -> Types.IntegerType.get()
    Schema.TypeName.DOUBLE -> Types.DoubleType.get()
    Schema.TypeName.FLOAT -> Types.FloatType.get()
    Schema.TypeName.BOOLEAN -> Types.BooleanType.get()
    Schema.TypeName.BYTES -> Types.BinaryType.get()
    Schema.TypeName.DATETIME -> Types.TimestampType.withoutZone()
    else -> throw IllegalArgumentException("Unsupported Beam field type: ${type.typeName}")
}

fun schemaOf(fields: List<String>): org.apache.iceberg.Schema {
    val struct = Types.StructType.of(
        parseSchemaFields(fields).mapIndexed { i, (name, type) ->
            Types.NestedField.optional(i + 1, name, toIcebergType(type))
        },
    )
    return org.apache.iceberg.Schema(struct.fields())
}

/** After loading the real table at runtime, validate the user-declared read fields to prevent a misspelled column name from being silently read as null. */
fun validateReadableSchema(
    actualSchema: org.apache.iceberg.Schema,
    fields: List<Pair<String, Schema.FieldType>>,
    tableName: String,
) {
    fields.forEach { (name, beamType) ->
        val actual = requireNotNull(actualSchema.findField(name)) {
            "Iceberg table [$tableName] does not contain the field [$name] declared in schema_fields; actual fields: " +
                actualSchema.columns().map { it.name() }
        }
        val expected = toIcebergType(beamType)
        require(actual.type() == expected) {
            "Iceberg table [$tableName] field [$name] has actual type ${actual.type()}, but schema_fields declares $beamType (corresponding to $expected)"
        }
    }
}

fun rowToRecord(icebergSchema: org.apache.iceberg.Schema, row: Row, fields: List<Pair<String, Schema.FieldType>>): Record {
    val record = GenericRecord.create(icebergSchema)
    fields.forEach { (name, type) ->
        record.setField(name, toIcebergValue(row.getValue(name), type))
    }
    return record
}

fun recordToRow(beamSchema: Schema, record: Record, fields: List<Pair<String, Schema.FieldType>>): Row {
    val builder = Row.withSchema(beamSchema)
    fields.forEach { (name, type) ->
        builder.addValue(fromIcebergValue(record.getField(name), type))
    }
    return builder.build()
}

private fun toIcebergValue(v: Any?, type: Schema.FieldType): Any? {
    if (v == null) return null
    return when (type.typeName) {
        Schema.TypeName.STRING -> v.toString()
        Schema.TypeName.INT64 -> decimal(v).longValueExact()
        Schema.TypeName.INT32 -> decimal(v).intValueExact()
        Schema.TypeName.DOUBLE -> decimal(v).toDouble().also { require(it.isFinite()) { "out of the finite DOUBLE range" } }
        Schema.TypeName.FLOAT -> decimal(v).toFloat().also { require(it.isFinite()) { "out of the finite FLOAT range" } }
        Schema.TypeName.BOOLEAN -> v as Boolean
        // An Iceberg binary column must be a ByteBuffer inside a GenericRecord; stuffing in a ByteArray only errors
        // at file-write time (or even writes out data that cannot be read back).
        Schema.TypeName.BYTES -> toByteBuffer(v)
        Schema.TypeName.DATETIME -> {
            val instant = v as org.joda.time.Instant
            LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(instant.millis), ZoneOffset.UTC)
        }
        else -> throw IllegalArgumentException("Unsupported Beam field type: ${type.typeName}")
    }
}

private fun fromIcebergValue(raw: Any?, type: Schema.FieldType): Any? {
    if (raw == null) return null
    return when (type.typeName) {
        Schema.TypeName.STRING -> raw.toString()
        Schema.TypeName.INT64 -> decimal(raw).longValueExact()
        Schema.TypeName.INT32 -> decimal(raw).intValueExact()
        Schema.TypeName.DOUBLE -> decimal(raw).toDouble().also { require(it.isFinite()) { "out of the finite DOUBLE range" } }
        Schema.TypeName.FLOAT -> decimal(raw).toFloat().also { require(it.isFinite()) { "out of the finite FLOAT range" } }
        Schema.TypeName.BOOLEAN -> raw as Boolean
        // Beam's BYTES field only accepts a ByteArray; passing the ByteBuffer that Iceberg gives us straight through
        // would cause a type error in Row.build().
        Schema.TypeName.BYTES -> toByteArray(raw)
        Schema.TypeName.DATETIME -> {
            val ldt = raw as LocalDateTime
            org.joda.time.Instant.ofEpochMilli(ldt.toInstant(ZoneOffset.UTC).toEpochMilli())
        }
        else -> throw IllegalArgumentException("Unsupported Beam field type: ${type.typeName}")
    }
}

/** A value on the Beam side -> Iceberg's binary representation. */
private fun toByteBuffer(v: Any): ByteBuffer = when (v) {
    is ByteBuffer -> v
    is ByteArray -> ByteBuffer.wrap(v)
    else -> throw IllegalArgumentException("An Iceberg BYTES write value must be a ByteArray/ByteBuffer, but was ${v.javaClass.name}")
}

/** Iceberg's binary representation -> a value on the Beam side. Read via duplicate() so the original buffer's position is untouched. */
private fun toByteArray(v: Any): ByteArray = when (v) {
    is ByteArray -> v
    is ByteBuffer -> {
        val copy = v.duplicate()
        ByteArray(copy.remaining()).also { copy.get(it) }
    }
    else -> throw IllegalArgumentException("An Iceberg binary read value must be a ByteArray/ByteBuffer, but was ${v.javaClass.name}")
}

private fun decimal(value: Any): BigDecimal = when (value) {
    is BigDecimal -> value
    is Number -> value.toString().toBigDecimal()
    else -> throw IllegalArgumentException("Value [$value] is not a number")
}
