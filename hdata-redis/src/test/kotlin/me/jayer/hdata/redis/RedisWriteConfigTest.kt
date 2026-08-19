package me.jayer.hdata.redis

import kotlin.test.Test
import kotlin.test.assertFailsWith

class RedisWriteConfigTest {

    @Test
    fun `默认配置合法`() {
        RedisWriteConfig().validate()
    }

    @Test
    fun `mode 非法报错`() {
        assertFailsWith<IllegalArgumentException> {
            RedisWriteConfig(mode = "zadd").validate()
        }
    }

    @Test
    fun `hset 模式缺 hash_field 报错`() {
        assertFailsWith<IllegalArgumentException> {
            RedisWriteConfig(mode = RedisWriteConfig.MODE_HSET, hashField = "").validate()
        }
    }

    @Test
    fun `key_field 为空报错`() {
        assertFailsWith<IllegalArgumentException> {
            RedisWriteConfig(keyField = "").validate()
        }
    }

    @Test
    fun `value_field 为空报错`() {
        // value 是写入的必填项，曾经这条校验缺失，缺了也不报错只会写出空值
        assertFailsWith<IllegalArgumentException> {
            RedisWriteConfig(valueField = "").validate()
        }
    }

    @Test
    fun `连接参数与 ttl 会校验`() {
        assertFailsWith<IllegalArgumentException> { RedisWriteConfig(host = "").validate() }
        assertFailsWith<IllegalArgumentException> { RedisWriteConfig(port = 0).validate() }
        assertFailsWith<IllegalArgumentException> { RedisWriteConfig(database = -1).validate() }
        assertFailsWith<IllegalArgumentException> { RedisWriteConfig(timeoutMs = 0).validate() }
        assertFailsWith<IllegalArgumentException> { RedisWriteConfig(ttlSeconds = 0).validate() }
        assertFailsWith<IllegalArgumentException> { RedisWriteConfig(ttlSeconds = -1).validate() }
    }
}
