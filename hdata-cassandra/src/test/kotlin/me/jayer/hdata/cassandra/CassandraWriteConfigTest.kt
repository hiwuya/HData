package me.jayer.hdata.cassandra

import kotlin.test.Test
import kotlin.test.assertFailsWith

class CassandraWriteConfigTest {

    @Test
    fun `default config is valid`() {
        CassandraWriteConfig(keyspace = "ks", table = "t").validate()
    }

    @Test
    fun `empty endpoints errors`() {
        assertFailsWith<IllegalArgumentException> {
            CassandraWriteConfig(endpoints = emptyList(), keyspace = "ks", table = "t").validate()
        }
    }

    @Test
    fun `blank keyspace errors`() {
        assertFailsWith<IllegalArgumentException> {
            CassandraWriteConfig(keyspace = "", table = "t").validate()
        }
    }

    @Test
    fun `blank table errors`() {
        assertFailsWith<IllegalArgumentException> {
            CassandraWriteConfig(keyspace = "ks", table = "").validate()
        }
    }

    @Test
    fun `zero batch_size errors`() {
        assertFailsWith<IllegalArgumentException> {
            CassandraWriteConfig(keyspace = "ks", table = "t", batchSize = 0).validate()
        }
    }

    @Test
    fun `negative max_retries errors`() {
        assertFailsWith<IllegalArgumentException> {
            CassandraWriteConfig(keyspace = "ks", table = "t", maxRetries = -1).validate()
        }
    }

    @Test
    fun `zero retry_delay_ms errors`() {
        assertFailsWith<IllegalArgumentException> {
            CassandraWriteConfig(keyspace = "ks", table = "t", retryDelayMs = 0).validate()
        }
    }

    @Test
    fun `zero timeouts errors`() {
        assertFailsWith<IllegalArgumentException> {
            CassandraWriteConfig(keyspace = "ks", table = "t", connectTimeoutMs = 0).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            CassandraWriteConfig(keyspace = "ks", table = "t", requestTimeoutMs = 0).validate()
        }
    }
}
