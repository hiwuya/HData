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
import org.redisson.api.RScript
import org.redisson.client.codec.StringCodec
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

/**
 * Writes Redis with `set`, `lpush`, `rpush`, `sadd`, or `hset`, optional expiry, and dead-letter support.
 *
 * Each DoFn creates one [RedissonClient] in `@Setup` and closes it in `@Teardown`. A failed command is routed
 * to dead letter when configured, otherwise it fails the job.
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
            val c = checkNotNull(client) { "Redis connection is not initialized" }
            if (config.ttlSeconds != null && config.mode != RedisWriteConfig.MODE_SET) {
                writeAtomicallyWithTtl(c, row, key, value, config.ttlSeconds)
                RECORDS_WRITTEN.inc()
                return
            }
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
        LOGGER.warn("Redis write failed; sending record to dead letter: {}", e.message)
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
        require(row.schema.hasField(field)) { "row written to Redis is missing field $field; available fields: ${row.schema.fieldNames}" }
        return row.getValue<Any?>(field)?.toString()
            ?: throw IllegalStateException("field $field is null; Redis keys and values must not be null")
    }

    /**
     * List, set, and hash operations have no SETEX-style TTL command. Lua makes the change and EXPIRE atomic;
     * separate requests could leave permanent data after a disconnect and make a retried list write duplicate values.
     */
    private fun writeAtomicallyWithTtl(
        client: RedissonClient,
        row: Row,
        key: String,
        value: String,
        ttlSeconds: Long,
    ) {
        val (script, args) = when (config.mode) {
            RedisWriteConfig.MODE_LPUSH -> PUSH_LEFT_WITH_TTL to arrayOf(value, ttlSeconds.toString())
            RedisWriteConfig.MODE_RPUSH -> PUSH_RIGHT_WITH_TTL to arrayOf(value, ttlSeconds.toString())
            RedisWriteConfig.MODE_SADD -> SET_ADD_WITH_TTL to arrayOf(value, ttlSeconds.toString())
            RedisWriteConfig.MODE_HSET -> HASH_SET_WITH_TTL to arrayOf(
                asString(row, config.hashField),
                value,
                ttlSeconds.toString(),
            )
            else -> error("mode=${config.mode} does not require a Lua TTL write")
        }
        client.getScript(StringCodec.INSTANCE).eval<Long>(
            RScript.Mode.READ_WRITE,
            script,
            RScript.ReturnType.INTEGER,
            listOf<Any>(key),
            *args,
        )
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(RedisWriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(RedisWriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(RedisWriteFn::class.java, "records_rejected")

        private const val PUSH_LEFT_WITH_TTL =
            "local r=redis.call('LPUSH',KEYS[1],ARGV[1]);redis.call('EXPIRE',KEYS[1],ARGV[2]);return r"
        private const val PUSH_RIGHT_WITH_TTL =
            "local r=redis.call('RPUSH',KEYS[1],ARGV[1]);redis.call('EXPIRE',KEYS[1],ARGV[2]);return r"
        private const val SET_ADD_WITH_TTL =
            "local r=redis.call('SADD',KEYS[1],ARGV[1]);redis.call('EXPIRE',KEYS[1],ARGV[2]);return r"
        private const val HASH_SET_WITH_TTL =
            "local r=redis.call('HSET',KEYS[1],ARGV[1],ARGV[2]);redis.call('EXPIRE',KEYS[1],ARGV[3]);return r"
    }
}
