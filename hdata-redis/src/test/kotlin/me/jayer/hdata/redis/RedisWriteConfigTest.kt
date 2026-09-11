package me.jayer.hdata.redis

import kotlin.test.Test
import kotlin.test.assertFailsWith

class RedisWriteConfigTest {

    @Test
    fun `default config is valid`() {
        RedisWriteConfig().validate()
    }

    @Test
    fun `invalid mode errors`() {
        assertFailsWith<IllegalArgumentException> {
            RedisWriteConfig(mode = "zadd").validate()
        }
    }

    @Test
    fun `hset mode missing hash_field errors`() {
        assertFailsWith<IllegalArgumentException> {
            RedisWriteConfig(mode = RedisWriteConfig.MODE_HSET, hashField = "").validate()
        }
    }

    @Test
    fun `a non-hset mode rejects a hash_field that would have no effect`() {
        assertFailsWith<IllegalArgumentException> {
            RedisWriteConfig(mode = RedisWriteConfig.MODE_SET, hashField = "ignored").validate()
        }
    }

    @Test
    fun `blank key_field errors`() {
        assertFailsWith<IllegalArgumentException> {
            RedisWriteConfig(keyField = "").validate()
        }
    }

    @Test
    fun `blank value_field errors`() {
        // value is required for a write; this check used to be missing, so a blank value_field silently wrote empty values
        assertFailsWith<IllegalArgumentException> {
            RedisWriteConfig(valueField = "").validate()
        }
    }

    @Test
    fun `connection params and ttl are validated`() {
        assertFailsWith<IllegalArgumentException> { RedisWriteConfig(host = "").validate() }
        assertFailsWith<IllegalArgumentException> { RedisWriteConfig(port = 0).validate() }
        assertFailsWith<IllegalArgumentException> { RedisWriteConfig(database = -1).validate() }
        assertFailsWith<IllegalArgumentException> { RedisWriteConfig(timeoutMs = 0).validate() }
        assertFailsWith<IllegalArgumentException> { RedisWriteConfig(ttlSeconds = 0).validate() }
        assertFailsWith<IllegalArgumentException> { RedisWriteConfig(ttlSeconds = -1).validate() }
    }
}
