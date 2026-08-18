package me.jayer.hdata.iceberg.internal

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.Record
import org.apache.iceberg.types.Types
import java.nio.ByteBuffer
import java.time.LocalDateTime
import java.time.ZoneOffset

/**
 * Iceberg 与 Beam 的字段类型映射：`name:TYPE` 列表 -> Beam schema / Iceberg schema，以及行互转。
 *
 * 支持 Iceberg 的基础类型（与 Beam 基本对齐）；嵌套 / list / map 暂不支持，按场景需要时再补。
 *
 * @author wuya
 */
fun parseSchemaFields(fields: List<String>): List<Pair<String, Schema.FieldType>> =
    fields.map { spec ->
        val parts = spec.split(":", limit = 2)
        val name = parts[0].trim()
        require(name.isNotBlank()) { "schema_fields 字段名不能为空: $spec" }
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
    else -> throw IllegalArgumentException("不支持的 Iceberg 字段类型: $type")
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
    else -> throw IllegalArgumentException("不支持的 Beam 字段类型: ${type.typeName}")
}

fun schemaOf(fields: List<String>): org.apache.iceberg.Schema {
    val struct = Types.StructType.of(
        parseSchemaFields(fields).mapIndexed { i, (name, type) ->
            Types.NestedField.optional(i + 1, name, toIcebergType(type))
        },
    )
    return org.apache.iceberg.Schema(struct.fields())
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
        Schema.TypeName.INT64 -> (v as Number).toLong()
        Schema.TypeName.INT32 -> (v as Number).toInt()
        Schema.TypeName.DOUBLE -> (v as Number).toDouble()
        Schema.TypeName.FLOAT -> (v as Number).toFloat()
        Schema.TypeName.BOOLEAN -> v as Boolean
        // Iceberg 的 binary 列在 GenericRecord 里必须是 ByteBuffer，塞 ByteArray 进去
        // 要到写文件时才报错（甚至写出读不回来的数据）
        Schema.TypeName.BYTES -> toByteBuffer(v)
        Schema.TypeName.DATETIME -> {
            val instant = v as org.joda.time.Instant
            LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(instant.millis), ZoneOffset.UTC)
        }
        else -> v.toString()
    }
}

private fun fromIcebergValue(raw: Any?, type: Schema.FieldType): Any? {
    if (raw == null) return null
    return when (type.typeName) {
        Schema.TypeName.STRING -> raw.toString()
        Schema.TypeName.INT64 -> (raw as Number).toLong()
        Schema.TypeName.INT32 -> (raw as Number).toInt()
        Schema.TypeName.DOUBLE -> (raw as Number).toDouble()
        Schema.TypeName.FLOAT -> (raw as Number).toFloat()
        Schema.TypeName.BOOLEAN -> raw as Boolean
        // Beam 的 BYTES 字段只接受 ByteArray，直接把 Iceberg 给的 ByteBuffer 传下去
        // 会在 Row.build() 时报类型错
        Schema.TypeName.BYTES -> toByteArray(raw)
        Schema.TypeName.DATETIME -> {
            val ldt = raw as LocalDateTime
            org.joda.time.Instant.ofEpochMilli(ldt.toInstant(ZoneOffset.UTC).toEpochMilli())
        }
        else -> raw.toString()
    }
}

/** Beam 侧的值 -> Iceberg 的 binary 表示。 */
private fun toByteBuffer(v: Any): Any = when (v) {
    is ByteBuffer -> v
    is ByteArray -> ByteBuffer.wrap(v)
    else -> v
}

/** Iceberg 的 binary 表示 -> Beam 侧的值。用 duplicate() 读，不动原 buffer 的 position。 */
private fun toByteArray(v: Any): Any = when (v) {
    is ByteArray -> v
    is ByteBuffer -> {
        val copy = v.duplicate()
        ByteArray(copy.remaining()).also { copy.get(it) }
    }
    else -> v
}
