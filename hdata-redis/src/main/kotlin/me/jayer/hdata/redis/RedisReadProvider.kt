package me.jayer.hdata.redis

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.redis.transform.REDIS_KEY_VALUE_SCHEMA
import me.jayer.hdata.redis.transform.REDIS_STREAM_SCHEMA
import me.jayer.hdata.redis.transform.RedisKeyReadFn
import me.jayer.hdata.redis.transform.RedisStreamEntry
import me.jayer.hdata.redis.transform.RedisStreamReadFn
import me.jayer.hdata.redis.transform.RANGE_STREAM
import me.jayer.hdata.redis.transform.SCAN_KEYS
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `ReadFromRedis`：从 Redis 读取。`scan`/`keys` 输出 `key`/`value`，`stream` 输出 `id`/`field`/`value`。
 *
 * 键的收集（SCAN / XRANGE）在 driver 端一次性完成（有界快照，贴合 HData 的批量同步定位），
 * 再经 `Create.of` + DoFn 逐条取值，连接在每个 DoFn 实例里独立建立，天然可序列化。
 *
 * @author wuya
 */
class RedisReadProvider : TypedTransformProvider<RedisReadConfig>(RedisReadConfig::class.java) {

    override fun identifier(): String = "ReadFromRedis"

    override fun description(): String = "从 Redis 读取（scan / keys / stream）"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(config: RedisReadConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return RedisSource(config)
    }
}

private class RedisSource(private val config: RedisReadConfig) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        val c = config
        return when (c.mode) {
            RedisReadConfig.MODE_STREAM -> {
                val entries = RANGE_STREAM.apply(c)
                if (entries.isEmpty()) {
                    begin.apply("Empty", Create.empty(REDIS_STREAM_SCHEMA)).setRowSchema(REDIS_STREAM_SCHEMA)
                } else {
                    begin.apply("Entries", Create.of(entries))
                        .apply("ToRow", ParDo.of(streamFn())).setRowSchema(REDIS_STREAM_SCHEMA)
                }
            }
            else -> {
                val keys = if (c.mode == RedisReadConfig.MODE_KEYS) c.keys else SCAN_KEYS.apply(c)
                if (keys.isEmpty()) {
                    begin.apply("Empty", Create.empty(REDIS_KEY_VALUE_SCHEMA)).setRowSchema(REDIS_KEY_VALUE_SCHEMA)
                } else {
                    begin.apply("Keys", Create.of(keys))
                        .apply("ToRow", ParDo.of(keyFn())).setRowSchema(REDIS_KEY_VALUE_SCHEMA)
                }
            }
        }
    }

    private fun keyFn() = RedisKeyReadFn(
        config.host, config.port, config.password, config.database, config.ssl, config.timeoutMs, REDIS_KEY_VALUE_SCHEMA,
    )

    private fun streamFn() = RedisStreamReadFn(REDIS_STREAM_SCHEMA)

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
