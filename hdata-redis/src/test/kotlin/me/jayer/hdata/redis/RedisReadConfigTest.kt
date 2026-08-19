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

    @Test
    fun `stream 模式 start_id 解析非法报错`() {
        // validate 在构图阶段就把 entry id 解析一遍，写错了（非 - / + / <毫秒>-<序号>）当场报错，
        // 不是等作业跑起来才发现 start_id/end_id 是死参数
        assertFailsWith<IllegalArgumentException> {
            RedisReadConfig(mode = RedisReadConfig.MODE_STREAM, stream = "s", startId = "abc").validate()
        }
        assertFailsWith<IllegalArgumentException> {
            RedisReadConfig(mode = RedisReadConfig.MODE_STREAM, stream = "s", startId = "100-x").validate()
        }
    }

    @Test
    fun `stream 模式 start_id 或 end_id 合法形式不报错`() {
        // `-` / `+` / `<毫秒>-<序号>` 都能被 parseStreamId 接受，且真的会被带进 XRANGE
        RedisReadConfig(mode = RedisReadConfig.MODE_STREAM, stream = "s", startId = "-", endId = "+").validate()
        RedisReadConfig(
            mode = RedisReadConfig.MODE_STREAM,
            stream = "s",
            startId = "1700000000000-0",
            endId = "1700000000000-5",
        ).validate()
    }

    @Test
    fun `连接参数 空 key 与负 stream id 会被拒绝`() {
        assertFailsWith<IllegalArgumentException> { RedisReadConfig(host = "").validate() }
        assertFailsWith<IllegalArgumentException> { RedisReadConfig(port = 70000).validate() }
        assertFailsWith<IllegalArgumentException> { RedisReadConfig(database = -1).validate() }
        assertFailsWith<IllegalArgumentException> { RedisReadConfig(timeoutMs = 0).validate() }
        assertFailsWith<IllegalArgumentException> {
            RedisReadConfig(mode = RedisReadConfig.MODE_KEYS, keys = listOf("a", " ")).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            RedisReadConfig(mode = RedisReadConfig.MODE_STREAM, stream = "s", startId = "1--1").validate()
        }
    }

    @Test
    fun `拒绝当前读取模式不会使用的参数`() {
        assertFailsWith<IllegalArgumentException> { RedisReadConfig(keys = listOf("a")).validate() }
        assertFailsWith<IllegalArgumentException> {
            RedisReadConfig(mode = RedisReadConfig.MODE_KEYS, keys = listOf("a"), stream = "events").validate()
        }
        assertFailsWith<IllegalArgumentException> {
            RedisReadConfig(mode = RedisReadConfig.MODE_STREAM, stream = "events", keyPattern = "user:*").validate()
        }
    }
}
