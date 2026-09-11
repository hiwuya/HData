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
 * A DoFn that captures a non-serializable object only blows up when the job is submitted; a unit test
 * that only calls `processElement` would never catch it, so every connector has at least one
 * `ensureSerializable` assertion (see the hdata-core convention).
 */
class RedisSerializationTest {

    @Test
    fun `DoFn and provider are serializable`() {
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
