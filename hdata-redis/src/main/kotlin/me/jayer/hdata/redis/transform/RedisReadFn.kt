package me.jayer.hdata.redis.transform

import me.jayer.hdata.redis.RedisReadConfig
import me.jayer.hdata.redis.internal.newRedisson
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.Row
import org.redisson.api.RedissonClient
import org.redisson.api.StreamMessageId
import org.redisson.api.stream.StreamReadArgs
import org.redisson.client.codec.StringCodec
import java.io.Serializable

/** A row emitted by `scan` or `keys`: `key` plus `value`. */
val REDIS_KEY_VALUE_SCHEMA: Schema = Schema.builder()
    .addNullableField("key", Schema.FieldType.STRING)
    .addNullableField("value", Schema.FieldType.STRING)
    .build()

/** A row emitted by `stream`: `id`, `field`, and `value`; one entry can expand to multiple rows. */
val REDIS_STREAM_SCHEMA: Schema = Schema.builder()
    .addNullableField("id", Schema.FieldType.STRING)
    .addNullableField("field", Schema.FieldType.STRING)
    .addNullableField("value", Schema.FieldType.STRING)
    .build()

/** Serializable snapshot of a stream entry collected by SCAN/XRANGE on the driver for a DoFn. */
data class RedisStreamEntry(val id: String, val fields: Map<String, String>) : Serializable

/** `scan` and `keys` mode: GET each key and emit `key` and `value`. */
class RedisKeyReadFn(
    private val host: String,
    private val port: Int,
    private val password: String,
    private val database: Int,
    private val ssl: Boolean,
    private val timeoutMs: Int,
    private val schema: Schema,
) : DoFn<String, Row>() {

    @Transient
    private var client: RedissonClient? = null

    @Setup
    fun setup() {
        client = newRedisson(host, port, password, database, ssl, timeoutMs)
    }

    @Teardown
    fun tearDown() {
        runCatching { client?.shutdown() }
        client = null
    }

    @ProcessElement
    fun processElement(@Element key: String, receiver: OutputReceiver<Row>) {
        val value = checkNotNull(client) { "Redis connection is not initialized" }
            .getBucket<String>(key, StringCodec.INSTANCE).get()
        receiver.output(Row.withSchema(schema).addValue(key).addValue(value).build())
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/** `stream` mode: expands a stream entry into `id`, `field`, and `value` rows. */
class RedisStreamReadFn(
    private val schema: Schema,
) : DoFn<RedisStreamEntry, Row>() {

    @ProcessElement
    fun processElement(@Element entry: RedisStreamEntry, receiver: OutputReceiver<Row>) {
        entry.fields.forEach { (field, value) ->
            receiver.output(
                Row.withSchema(schema).addValue(entry.id).addValue(field).addValue(value).build(),
            )
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/** Collects matching keys once on the driver as a bounded snapshot. */
val SCAN_KEYS: SerializableFunction<RedisReadConfig, List<String>> = SerializableFunction { config ->
    val client = newRedisson(config.host, config.port, config.password, config.database, config.ssl, config.timeoutMs)
    try {
        client.keys.getKeysByPattern(config.keyPattern).toList()
    } finally {
        client.shutdown()
    }
}

/**
 * Reads the stream `[start_id, end_id]` range once on the driver as a bounded snapshot.
 *
 * [parseStreamId] makes `start_id` and `end_id` actual XRANGE boundaries. `-` and `+` select the ends;
 * other values use `<millis>-<sequence>`, and empty values fall back to [StreamMessageId.MIN] or [StreamMessageId.MAX].
 */
val RANGE_STREAM: SerializableFunction<RedisReadConfig, List<RedisStreamEntry>> =
    SerializableFunction { config ->
        val client = newRedisson(config.host, config.port, config.password, config.database, config.ssl, config.timeoutMs)
        try {
            client.getStream<String, String>(config.stream, StringCodec.INSTANCE)
                .range(parseStreamId(config.startId, StreamMessageId.MIN), parseStreamId(config.endId, StreamMessageId.MAX))
                .map { (id, fields) -> RedisStreamEntry(id.toString(), fields) }
        } finally {
            client.shutdown()
        }
    }

/**
 * Parses a stream entry ID.
 *
 * `-` and `+` represent the stream boundaries. Other values use `<millis>-<sequence>`; a missing sequence is zero.
 *
 * @param fallback default for an empty value: [StreamMessageId.MIN] for a start or [StreamMessageId.MAX] for an end
 */
fun parseStreamId(value: String, fallback: StreamMessageId): StreamMessageId {
    val text = value.trim()
    return when (text) {
        "" -> fallback
        "-" -> StreamMessageId.MIN
        "+" -> StreamMessageId.MAX
        else -> {
            val parts = text.split("-", limit = 2)
            val millis = parts[0].toLongOrNull()
                ?: throw IllegalArgumentException("Cannot parse Redis stream entry ID: $value; expected - / + / 1700000000000 / 1700000000000-0")
            val sequence = if (parts.size == 2) {
                parts[1].toLongOrNull()
                    ?: throw IllegalArgumentException("Cannot parse the Redis stream entry ID sequence: $value")
            } else {
                0L
            }
            require(millis >= 0 && sequence >= 0) { "Redis stream entry ID must not be negative: $value" }
            StreamMessageId(millis, sequence)
        }
    }
}

/** Compares XRANGE boundaries because Redisson's [StreamMessageId] does not implement Comparable. */
fun compareStreamIds(left: StreamMessageId, right: StreamMessageId): Int {
    if (left === right) return 0
    if (left === StreamMessageId.MIN || right === StreamMessageId.MAX) return -1
    if (left === StreamMessageId.MAX || right === StreamMessageId.MIN) return 1
    val millis = left.id0.compareTo(right.id0)
    return if (millis != 0) millis else left.id1.compareTo(right.id1)
}
