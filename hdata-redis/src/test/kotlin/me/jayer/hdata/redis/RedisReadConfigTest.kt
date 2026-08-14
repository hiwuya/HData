package me.jayer.hdata.redis

import kotlin.test.Test
import kotlin.test.assertFailsWith

class RedisReadConfigTest {

    @Test
    fun `默认配置合法`() {
        RedisReadConfig().validate()
    }

    @Test
    fun `mode 非法报错`() {
        assertFailsWith<IllegalArgumentException> {
            RedisReadConfig(mode = "bogus").validate()
        }
    }

    @Test
    fun `keys 模式缺 keys 报错`() {
        assertFailsWith<IllegalArgumentException> {
            RedisReadConfig(mode = RedisReadConfig.MODE_KEYS).validate()
        }
    }

    @Test
    fun `stream 模式缺 stream 报错`() {
        assertFailsWith<IllegalArgumentException> {
            RedisReadConfig(mode = RedisReadConfig.MODE_STREAM).validate()
        }
    }

    @Test
    fun `scan 模式缺 key_pattern 报错`() {
        assertFailsWith<IllegalArgumentException> {
            RedisReadConfig(mode = RedisReadConfig.MODE_SCAN, keyPattern = "").validate()
        }
    }
}
