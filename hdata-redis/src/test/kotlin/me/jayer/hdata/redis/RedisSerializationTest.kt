package me.jayer.hdata.redis

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.redis.transform.REDIS_KEY_VALUE_SCHEMA
import me.jayer.hdata.redis.transform.REDIS_STREAM_SCHEMA
import me.jayer.hdata.redis.transform.RedisKeyReadFn
import me.jayer.hdata.redis.transform.RedisStreamReadFn
import me.jayer.hdata.redis.transform.RedisWriteFn
import org.apache.beam.sdk.util.SerializableUtils
import org.junit.jupiter.api.Test

/**
 * 连接器 DoFn 捕获了不可序列化的对象只会在提交作业时炸，单测只调 `processElement` 永远发现不了，
 * 所以每个连接器至少有一个 `ensureSerializable` 断言（参见 hdata-core 的约定）。
 */
class RedisSerializationTest {

    @Test
    fun `DoFn 与 provider 可序列化`() {
        val config = RedisReadConfig(host = "localhost", port = 6379)
        SerializableUtils.ensureSerializable(
            RedisKeyReadFn("localhost", 6379, "", 0, false, 5000, REDIS_KEY_VALUE_SCHEMA)
        )
        SerializableUtils.ensureSerializable(
            RedisStreamReadFn(REDIS_STREAM_SCHEMA)
        )
        SerializableUtils.ensureSerializable(
            RedisWriteFn(
                RedisWriteConfig(),
                ErrorSchemas.of(REDIS_KEY_VALUE_SCHEMA),
                deadLetter = true,
                transformName = "WriteToRedis",
            )
        )
    }
}
