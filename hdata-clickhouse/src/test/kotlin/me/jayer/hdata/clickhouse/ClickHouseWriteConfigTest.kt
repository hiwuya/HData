package me.jayer.hdata.clickhouse

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ClickHouseWriteConfigTest {

    @Test
    fun `default config is valid`() {
        ClickHouseWriteConfig(table = "events").validate()
    }

    @Test
    fun `blank endpoint errors`() {
        assertFailsWith<IllegalArgumentException> {
            ClickHouseWriteConfig(endpoint = "", table = "events").validate()
        }
    }

    @Test
    fun `blank database errors`() {
        assertFailsWith<IllegalArgumentException> {
            ClickHouseWriteConfig(database = "", table = "events").validate()
        }
    }

    @Test
    fun `blank username errors`() {
        assertFailsWith<IllegalArgumentException> {
            ClickHouseWriteConfig(username = "", table = "events").validate()
        }
    }

    @Test
    fun `blank table errors`() {
        assertFailsWith<IllegalArgumentException> {
            ClickHouseWriteConfig(table = "").validate()
        }
    }

    @Test
    fun `zero batch_size errors`() {
        assertFailsWith<IllegalArgumentException> {
            ClickHouseWriteConfig(table = "events", batchSize = 0).validate()
        }
    }

    @Test
    fun `negative max_retries errors`() {
        assertFailsWith<IllegalArgumentException> {
            ClickHouseWriteConfig(table = "events", maxRetries = -1).validate()
        }
    }

    @Test
    fun `zero retry_delay_ms errors`() {
        assertFailsWith<IllegalArgumentException> {
            ClickHouseWriteConfig(table = "events", retryDelayMs = 0).validate()
        }
    }

    @Test
    fun `zero connect_timeout_ms errors`() {
        assertFailsWith<IllegalArgumentException> {
            ClickHouseWriteConfig(table = "events", connectTimeoutMs = 0).validate()
        }
    }

    @Test
    fun `zero socket_timeout_ms errors`() {
        assertFailsWith<IllegalArgumentException> {
            ClickHouseWriteConfig(table = "events", socketTimeoutMs = 0).validate()
        }
    }

    @Test
    fun `resolved_columns uses row schema when column_names is empty`() {
        val config = ClickHouseWriteConfig(table = "events")
        val result = config.resolvedColumns(listOf("id", "name", "value"))
        assertEquals(listOf("id", "name", "value"), result)
    }

    @Test
    fun `resolved_columns uses explicit column_names`() {
        val config = ClickHouseWriteConfig(table = "events", columnNames = listOf("a", "b", "c"))
        val result = config.resolvedColumns(listOf("id", "name", "value"))
        assertEquals(listOf("a", "b", "c"), result)
    }

    @Test
    fun `resolved_columns rejects mismatched column_names size`() {
        val config = ClickHouseWriteConfig(table = "events", columnNames = listOf("a", "b"))
        assertFailsWith<IllegalArgumentException> {
            config.resolvedColumns(listOf("id", "name", "value"))
        }
    }
}
