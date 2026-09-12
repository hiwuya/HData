package me.jayer.hdata.cassandra

import kotlin.test.Test
import kotlin.test.assertFailsWith

class CassandraReadConfigTest {

    @Test
    fun `default config is valid`() {
        CassandraReadConfig(keyspace = "ks", query = "SELECT * FROM t").validate()
    }

    @Test
    fun `empty endpoints errors`() {
        assertFailsWith<IllegalArgumentException> {
            CassandraReadConfig(endpoints = emptyList(), keyspace = "ks", query = "SELECT 1").validate()
        }
    }

    @Test
    fun `blank endpoint errors`() {
        assertFailsWith<IllegalArgumentException> {
            CassandraReadConfig(endpoints = listOf(""), keyspace = "ks", query = "SELECT 1").validate()
        }
    }

    @Test
    fun `endpoint without port errors`() {
        assertFailsWith<IllegalArgumentException> {
            CassandraReadConfig(endpoints = listOf("localhost"), keyspace = "ks", query = "SELECT 1").validate()
        }
    }

    @Test
    fun `endpoint with invalid port errors`() {
        assertFailsWith<IllegalArgumentException> {
            CassandraReadConfig(endpoints = listOf("localhost:99999"), keyspace = "ks", query = "SELECT 1").validate()
        }
    }

    @Test
    fun `blank keyspace errors`() {
        assertFailsWith<IllegalArgumentException> {
            CassandraReadConfig(keyspace = "", query = "SELECT 1").validate()
        }
    }

    @Test
    fun `blank query errors`() {
        assertFailsWith<IllegalArgumentException> {
            CassandraReadConfig(keyspace = "ks", query = "").validate()
        }
    }

    @Test
    fun `negative max_rows errors`() {
        assertFailsWith<IllegalArgumentException> {
            CassandraReadConfig(keyspace = "ks", query = "SELECT 1", maxRows = -1).validate()
        }
    }

    @Test
    fun `zero fetch_size errors`() {
        assertFailsWith<IllegalArgumentException> {
            CassandraReadConfig(keyspace = "ks", query = "SELECT 1", fetchSize = 0).validate()
        }
    }

    @Test
    fun `zero timeouts errors`() {
        assertFailsWith<IllegalArgumentException> {
            CassandraReadConfig(keyspace = "ks", query = "SELECT 1", connectTimeoutMs = 0).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            CassandraReadConfig(keyspace = "ks", query = "SELECT 1", requestTimeoutMs = 0).validate()
        }
    }

    @Test
    fun `valid endpoint with multiple entries`() {
        CassandraReadConfig(
            endpoints = listOf("host1:9042", "host2:9042"),
            keyspace = "ks",
            query = "SELECT 1",
        ).validate()
    }

    @Test
    fun `parallel scan requires partition key`() {
        assertFailsWith<IllegalArgumentException> {
            CassandraReadConfig(keyspace = "ks", query = "SELECT * FROM t", parallelScanSegments = 2).validate()
        }
        CassandraReadConfig(
            keyspace = "ks", query = "SELECT * FROM t", partitionKeyColumn = "id", parallelScanSegments = 2,
        ).validate()
    }

    @Test
    fun `parallel scan segments must be positive`() {
        assertFailsWith<IllegalArgumentException> {
            CassandraReadConfig(keyspace = "ks", query = "SELECT 1", parallelScanSegments = 0).validate()
        }
    }
}
