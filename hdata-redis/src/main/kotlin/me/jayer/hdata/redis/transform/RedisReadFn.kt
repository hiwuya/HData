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

/** `scan` / `keys` 模式读出的一行：`key` + `value`。 */
val REDIS_KEY_VALUE_SCHEMA: Schema = Schema.builder()
    .addNullableField("key", Schema.FieldType.STRING)
    .addNullableField("value", Schema.FieldType.STRING)
    .build()

/** `stream` 模式读出的一行：`id` + `field` + `value`（一个 stream 条目展开成多行）。 */
val REDIS_STREAM_SCHEMA: Schema = Schema.builder()
    .addNullableField("id", Schema.FieldType.STRING)
    .addNullableField("field", Schema.FieldType.STRING)
    .addNullableField("value", Schema.FieldType.STRING)
    .build()

/** 可被 Beam 序列化、在 driver 端 SCAN/XRANGE 收集后传给 DoFn 的 stream 条目快照。 */
data class RedisStreamEntry(val id: String, val fields: Map<String, String>) : Serializable

/** `scan`/`keys` 模式：按 key 逐个 GET，输出 `key`/`value`。 */
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
        val value = checkNotNull(client) { "Redis 连接未初始化" }
            .getBucket<String>(key, StringCodec.INSTANCE).get()
        receiver.output(Row.withSchema(schema).addValue(key).addValue(value).build())
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/** `stream` 模式：把一个 stream 条目展开成多行（`id`/`field`/`value`）。 */
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

/** driver 端一次性收集所有匹配 key（有界快照，适合批量同步）。 */
val SCAN_KEYS: SerializableFunction<RedisReadConfig, List<String>> = SerializableFunction { config ->
    val client = newRedisson(config.host, config.port, config.password, config.database, config.ssl, config.timeoutMs)
    try {
        client.keys.getKeysByPattern(config.keyPattern).toList()
    } finally {
        client.shutdown()
    }
}

/**
 * driver 端一次性读取 stream 的 `[start_id, end_id]` 区间（有界快照）。
 *
 * `start_id` / `end_id` 之前是**收下就丢掉**的死参数：无论填什么都按 `MIN`..`MAX` 全量读，
 * 用户以为自己在读一段增量，实际每次都是全量。
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
 * 解析 stream 的 entry id。
 *
 * `-` / `+` 是 Redis 里表示首尾的写法，其余按 `<毫秒>-<序号>` 解析；只给毫秒时序号补 0。
 *
 * @param fallback 留空时用的默认值（起点是 [StreamMessageId.MIN]、终点是 [StreamMessageId.MAX]）
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
                ?: throw IllegalArgumentException("无法解析 Redis stream entry id: $value，合法写法: - / + / 1700000000000 / 1700000000000-0")
            val sequence = if (parts.size == 2) {
                parts[1].toLongOrNull()
                    ?: throw IllegalArgumentException("无法解析 Redis stream entry id 的序号部分: $value")
            } else {
                0L
            }
            StreamMessageId(millis, sequence)
        }
    }
}
