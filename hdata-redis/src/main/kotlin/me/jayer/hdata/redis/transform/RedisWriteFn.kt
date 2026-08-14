package me.jayer.hdata.redis.transform

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.redis.RedisWriteConfig
import me.jayer.hdata.redis.internal.newRedisson
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.redisson.api.RedissonClient
import org.redisson.client.codec.StringCodec
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

/**
 * 写入 Redis，支持 `set` / `lpush` / `rpush` / `sadd` / `hset` 与可选的过期时间，支持死信输出。
 *
 * 每个 DoFn 实例持有一份 [RedissonClient] 连接（`@Setup` 建、`@Teardown` 关），逐条执行命令；
 * 单条命令失败不拖垮整个 bundle，而是按 `error_handling` 决定进死信还是让作业失败。
 *
 * @author wuya
 */
class RedisWriteFn(
    private val config: RedisWriteConfig,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
) : DoFn<Row, Row>() {

    @Transient
    private var client: RedissonClient? = null

    @Transient
    private var failures: MutableList<ValueInSingleWindow<Row>>? = null

    @Setup
    fun setup() {
        client = newRedisson(config.host, config.port, config.password, config.database, config.ssl, config.timeoutMs)
        failures = mutableListOf()
    }

    @Teardown
    fun tearDown() {
        runCatching { client?.shutdown() }
        client = null
    }

    @StartBundle
    fun startBundle() {
        failures?.clear()
    }

    @ProcessElement
    fun processElement(
        @Element row: Row,
        @Timestamp timestamp: org.joda.time.Instant,
        window: BoundedWindow,
        pane: PaneInfo,
    ) {
        val visw = ValueInSingleWindow.of(row, timestamp, window, pane)
        try {
            val key = asString(row, config.keyField)
            val value = asString(row, config.valueField)
            val c = checkNotNull(client) { "Redis 连接未初始化" }
            when (config.mode) {
                RedisWriteConfig.MODE_SET -> {
                    val bucket = c.getBucket<String>(key, StringCodec.INSTANCE)
                    if (config.ttlSeconds != null) {
                        bucket.set(value, config.ttlSeconds, TimeUnit.SECONDS)
                    } else {
                        bucket.set(value)
                    }
                }
                RedisWriteConfig.MODE_LPUSH -> c.getList<String>(key, StringCodec.INSTANCE).add(0, value)
                RedisWriteConfig.MODE_RPUSH -> c.getList<String>(key, StringCodec.INSTANCE).add(value)
                RedisWriteConfig.MODE_SADD -> c.getSet<String>(key, StringCodec.INSTANCE).add(value)
                RedisWriteConfig.MODE_HSET -> {
                    val field = asString(row, config.hashField)
                    c.getMap<String, String>(key, StringCodec.INSTANCE).put(field, value)
                }
            }
            if (config.ttlSeconds != null && config.mode != RedisWriteConfig.MODE_SET) {
                c.keys.expire(key, config.ttlSeconds, TimeUnit.SECONDS)
            }
            RECORDS_WRITTEN.inc()
        } catch (e: Exception) {
            reject(visw, e)
        }
    }

    @FinishBundle
    fun finishBundle(context: FinishBundleContext) {
        checkNotNull(failures).forEach { context.output(it.value, it.timestamp, it.window) }
        failures?.clear()
    }

    private fun reject(record: ValueInSingleWindow<Row>, e: Exception) {
        if (!deadLetter) {
            throw e
        }
        LOGGER.warn("写入 Redis 失败，转入死信: {}", e.message)
        RECORDS_REJECTED.inc()
        checkNotNull(failures).add(
            ValueInSingleWindow.of(
                ErrorSchemas.failure(errorSchema, record.value, e, transformName),
                record.timestamp,
                record.window,
                record.paneInfo,
            )
        )
    }

    private fun asString(row: Row, field: String): String {
        require(row.schema.hasField(field)) { "写 Redis 的行缺少字段 $field，现有字段: ${row.schema.fieldNames}" }
        return row.getValue<Any?>(field)?.toString()
            ?: throw IllegalStateException("字段 $field 为 null，Redis 的 key/value 不允许 null")
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(RedisWriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(RedisWriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(RedisWriteFn::class.java, "records_rejected")
    }
}
