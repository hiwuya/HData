package me.jayer.hdata.prometheus

import kotlin.test.Test
import kotlin.test.assertFailsWith

class PrometheusReadConfigTest {

    @Test
    fun `default config is valid`() {
        PrometheusReadConfig(endpoint = "http://localhost:9090", query = "up").validate()
    }

    @Test
    fun `blank endpoint errors`() {
        assertFailsWith<IllegalArgumentException> {
            PrometheusReadConfig(endpoint = "", query = "up").validate()
        }
    }

    @Test
    fun `blank query errors`() {
        assertFailsWith<IllegalArgumentException> {
            PrometheusReadConfig(endpoint = "http://localhost:9090", query = "").validate()
        }
    }

    @Test
    fun `zero connect_timeout_ms errors`() {
        assertFailsWith<IllegalArgumentException> {
            PrometheusReadConfig(
                endpoint = "http://localhost:9090",
                query = "up",
                connectTimeoutMs = 0,
            ).validate()
        }
    }

    @Test
    fun `zero read_timeout_ms errors`() {
        assertFailsWith<IllegalArgumentException> {
            PrometheusReadConfig(
                endpoint = "http://localhost:9090",
                query = "up",
                readTimeoutMs = 0,
            ).validate()
        }
    }
}
