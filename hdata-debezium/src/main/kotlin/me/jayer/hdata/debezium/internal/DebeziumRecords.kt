package me.jayer.hdata.debezium.internal

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import org.apache.kafka.connect.data.Schema as ConnectSchema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import tools.jackson.databind.ObjectMapper
import java.util.Base64

/**
 * 把 Debezium 的 [SourceRecord] 转成 HData 的 [Row]。
 *
 * Debezium 连接器（mysql/postgres）吐出的 `value` 是一个 Envelope（含 `op` /
 * `before` / `after` / `source` / `ts_ms` 字段），标准模式直接映射即可；
 * 非 Envelope 的 SourceRecord（如 Debezium 自带的 `SimpleSourceConnector` 测试源）
 * 兜底成 `op="r"` 且整行落在 `after` 上，便于在没有真实数据库时也能端到端跑通。
 */
object DebeziumRecords {

    val SCHEMA: Schema = Schema.builder()
        .addStringField("op")
        .addNullableField("key", Schema.FieldType.STRING)
        .addNullableField("before", Schema.FieldType.STRING)
        .addNullableField("after", Schema.FieldType.STRING)
        .addNullableField("source", Schema.FieldType.STRING)
        .addNullableField("ts_ms", Schema.FieldType.INT64)
        .build()

    private val mapper = ObjectMapper()

    fun toRow(record: SourceRecord): Row? {
        val value = record.value() ?: return null
        val valueSchema = record.valueSchema()
        if (value is Struct && valueSchema != null && isEnvelope(valueSchema)) {
            val op = value.getString("op") ?: return null
            return buildRow(
                op = op,
                key = record.key(),
                keySchema = record.keySchema(),
                before = value.get("before"),
                beforeSchema = value.schema().field("before")?.schema(),
                after = value.get("after"),
                afterSchema = value.schema().field("after")?.schema(),
                source = value.get("source"),
                sourceSchema = value.schema().field("source")?.schema(),
                tsMs = if (value.schema().field("ts_ms") != null) value.getInt64("ts_ms") else null,
            )
        }
        return buildRow(
            op = "r",
            key = record.key(),
            keySchema = record.keySchema(),
            before = null,
            beforeSchema = null,
            after = value,
            afterSchema = valueSchema,
            source = null,
            sourceSchema = null,
            tsMs = null,
        )
    }

    private fun isEnvelope(schema: ConnectSchema): Boolean =
        schema.type() == ConnectSchema.Type.STRUCT && schema.field("op") != null

    private fun buildRow(
        op: String,
        key: Any?,
        keySchema: ConnectSchema?,
        before: Any?,
        beforeSchema: ConnectSchema?,
        after: Any?,
        afterSchema: ConnectSchema?,
        source: Any?,
        sourceSchema: ConnectSchema?,
        tsMs: Any?,
    ): Row {
        return Row.withSchema(SCHEMA)
            .addValue(op)
            .addValue(key?.let { toJson(it, keySchema) })
            .addValue(before?.let { toJson(it, beforeSchema) })
            .addValue(after?.let { toJson(it, afterSchema) })
            .addValue(source?.let { toJson(it, sourceSchema) })
            .addValue(tsMs)
            .build()
    }

    private fun toJson(value: Any?, schema: ConnectSchema?): String? {
        if (value == null) return null
        return mapper.writeValueAsString(connectToNode(value, schema))
    }

    @Suppress("UNCHECKED_CAST")
    private fun connectToNode(value: Any?, schema: ConnectSchema?): Any? {
        if (value == null) return null
        return when {
            value is Struct -> {
                val s = value.schema()
                s.fields().associate { f -> f.name() to connectToNode(value.get(f.name()), f.schema()) }
            }
            value is Map<*, *> -> {
                val vs = schema?.valueSchema()
                value.entries.associate { (k, v) -> k.toString() to connectToNode(v, vs) }
            }
            value is Collection<*> -> {
                val vs = schema?.valueSchema()
                value.map { connectToNode(it, vs) }
            }
            value is ByteArray -> Base64.getEncoder().encodeToString(value)
            value is java.nio.ByteBuffer -> Base64.getEncoder().encodeToString(byteBufferToArray(value))
            else -> value
        }
    }

    private fun byteBufferToArray(bb: java.nio.ByteBuffer): ByteArray {
        val copy = bb.duplicate()
        val a = ByteArray(copy.remaining())
        copy.get(a)
        return a
    }
}
