package me.jayer.hdata.redis

import kotlin.test.Test
import kotlin.test.assertFailsWith

class RedisReadConfigTest {

    @Test
    fun `default config is valid`() {
        RedisReadConfig().validate()
    }

    @Test
    fun `invalid mode errors`() {
        assertFailsWith<IllegalArgumentException> {
            RedisReadConfig(mode = "bogus").validate()
        }
    }

    @Test
    fun `keys mode missing keys errors`() {
        assertFailsWith<IllegalArgumentException> {
            RedisReadConfig(mode = RedisReadConfig.MODE_KEYS).validate()
        }
    }

    @Test
    fun `stream mode missing stream errors`() {
        assertFailsWith<IllegalArgumentException> {
            RedisReadConfig(mode = RedisReadConfig.MODE_STREAM).validate()
        }
    }

    @Test
    fun `scan mode missing key_pattern errors`() {
        assertFailsWith<IllegalArgumentException> {
            RedisReadConfig(mode = RedisReadConfig.MODE_SCAN, keyPattern = "").validate()
        }
    }

    @Test
    fun `stream mode rejects an unparseable start_id`() {
        // validate() parses the entry id at graph-construction time, so a malformed value (anything other
        // than - / + / <millis>-<seq>) errors immediately, instead of start_id/end_id silently being dead
        // parameters until the job actually runs
        assertFailsWith<IllegalArgumentException> {
            RedisReadConfig(mode = RedisReadConfig.MODE_STREAM, stream = "s", startId = "abc").validate()
        }
        assertFailsWith<IllegalArgumentException> {
            RedisReadConfig(mode = RedisReadConfig.MODE_STREAM, stream = "s", startId = "100-x").validate()
        }
    }

    @Test
    fun `stream mode accepts valid start_id and end_id forms`() {
        // `-` / `+` / `<millis>-<seq>` are all accepted by parseStreamId and really do get passed into XRANGE
        RedisReadConfig(mode = RedisReadConfig.MODE_STREAM, stream = "s", startId = "-", endId = "+").validate()
        RedisReadConfig(
            mode = RedisReadConfig.MODE_STREAM,
            stream = "s",
            startId = "1700000000000-0",
            endId = "1700000000000-5",
        ).validate()
    }

    @Test
    fun `stream mode rejects an inverted id range`() {
        val error = assertFailsWith<IllegalArgumentException> {
            RedisReadConfig(
                mode = RedisReadConfig.MODE_STREAM,
                stream = "s",
                startId = "1700000000000-6",
                endId = "1700000000000-5",
            ).validate()
        }
        kotlin.test.assertTrue("start_id" in error.message!! && "end_id" in error.message!!)

        assertFailsWith<IllegalArgumentException> {
            RedisReadConfig(mode = RedisReadConfig.MODE_STREAM, stream = "s", startId = "+", endId = "-").validate()
        }
    }

    @Test
    fun `connection params, a blank key, and a negative stream id are all rejected`() {
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
    fun `rejects parameters unused by the current read mode`() {
        assertFailsWith<IllegalArgumentException> { RedisReadConfig(keys = listOf("a")).validate() }
        assertFailsWith<IllegalArgumentException> {
            RedisReadConfig(mode = RedisReadConfig.MODE_KEYS, keys = listOf("a"), stream = "events").validate()
        }
        assertFailsWith<IllegalArgumentException> {
            RedisReadConfig(mode = RedisReadConfig.MODE_STREAM, stream = "events", keyPattern = "user:*").validate()
        }
    }
}
