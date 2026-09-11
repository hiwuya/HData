package me.jayer.hdata.clickhouse

import kotlin.test.Test
import kotlin.test.assertFailsWith

class ClickHouseReadConfigTest {

    @Test
    fun `default config is valid`() {
        ClickHouseReadConfig(query = "SELECT 1").validate()
    }

    @Test
    fun `blank endpoint errors`() {
        assertFailsWith<IllegalArgumentException> {
            ClickHouseReadConfig(endpoint = "", query = "SELECT 1").validate()
        }
    }

    @Test
    fun `blank database errors`() {
        assertFailsWith<IllegalArgumentException> {
            ClickHouseReadConfig(database = "", query = "SELECT 1").validate()
        }
    }

    @Test
    fun `blank username errors`() {
        assertFailsWith<IllegalArgumentException> {
            ClickHouseReadConfig(username = "", query = "SELECT 1").validate()
        }
    }

    @Test
    fun `blank query errors`() {
        assertFailsWith<IllegalArgumentException> {
            ClickHouseReadConfig(query = "").validate()
        }
    }

    @Test
    fun `negative max_rows errors`() {
        assertFailsWith<IllegalArgumentException> {
            ClickHouseReadConfig(maxRows = -1, query = "SELECT 1").validate()
        }
    }

    @Test
    fun `zero connect_timeout_ms errors`() {
        assertFailsWith<IllegalArgumentException> {
            ClickHouseReadConfig(connectTimeoutMs = 0, query = "SELECT 1").validate()
        }
    }

    @Test
    fun `zero socket_timeout_ms errors`() {
        assertFailsWith<IllegalArgumentException> {
            ClickHouseReadConfig(socketTimeoutMs = 0, query = "SELECT 1").validate()
        }
    }
}
