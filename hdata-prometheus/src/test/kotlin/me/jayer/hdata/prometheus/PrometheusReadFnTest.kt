package me.jayer.hdata.prometheus

import com.sun.net.httpserver.HttpServer
import me.jayer.hdata.prometheus.transform.PROMETHEUS_READ_SCHEMA
import me.jayer.hdata.prometheus.transform.PrometheusReadFn
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PrometheusReadFnTest {

    private var server: HttpServer? = null

    @AfterEach
    fun tearDown() {
        server?.stop(0)
        server = null
    }

    private fun startMockServer(responseJson: String): HttpServer {
        val httpServer = HttpServer.create(InetSocketAddress(0), 0)
        httpServer.createContext("/api/v1/query") { exchange ->
            val response = responseJson.toByteArray(Charsets.UTF_8)
            exchange.responseHeaders.set("Content-Type", "application/json")
            exchange.sendResponseHeaders(200, response.size.toLong())
            exchange.responseBody.use { it.write(response) }
        }
        httpServer.start()
        server = httpServer
        return httpServer
    }

    @Test
    fun `parses vector response correctly`() {
        val json = """
            {
              "status": "success",
              "data": {
                "resultType": "vector",
                "result": [
                  {
                    "metric": {"__name__": "up", "job": "prometheus", "instance": "localhost:9090"},
                    "value": [1234567890.0, "1"]
                  }
                ]
              }
            }
        """.trimIndent()

        val httpServer = startMockServer(json)
        val port = httpServer.address.port

        val config = PrometheusReadConfig(
            endpoint = "http://localhost:$port",
            query = "up",
            connectTimeoutMs = 5_000,
            readTimeoutMs = 5_000,
        )

        val pipeline = Pipeline.create()
        val output = pipeline
            .apply("Trigger", Create.of(listOf(1)))
            .apply("ReadFromPrometheus", ParDo.of(PrometheusReadFn(config)))
            .setRowSchema(PROMETHEUS_READ_SCHEMA)

        PAssert.that(output).satisfies { rows ->
            val rowList = rows.toList()
            assertEquals(1, rowList.size)
            val row = rowList[0]
            assertEquals("up", row.getString("metric_name"))
            val labels = row.getMap<String, String>("labels")
            assertEquals("prometheus", labels?.get("job"))
            assertEquals("localhost:9090", labels?.get("instance"))
            assertEquals(1.0, row.getDouble("value")!!, 0.001)
            assertEquals(1234567890.0, row.getDouble("timestamp")!!, 0.001)
            null
        }
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `parses multiple series`() {
        val json = """
            {
              "status": "success",
              "data": {
                "resultType": "vector",
                "result": [
                  {
                    "metric": {"__name__": "up", "job": "node", "instance": "host1:9100"},
                    "value": [1000.0, "1"]
                  },
                  {
                    "metric": {"__name__": "up", "job": "prometheus", "instance": "host2:9090"},
                    "value": [2000.0, "1"]
                  }
                ]
              }
            }
        """.trimIndent()

        val httpServer = startMockServer(json)
        val port = httpServer.address.port

        val config = PrometheusReadConfig(
            endpoint = "http://localhost:$port",
            query = "up",
        )

        val pipeline = Pipeline.create()
        val output = pipeline
            .apply("Trigger", Create.of(listOf(1)))
            .apply("ReadFromPrometheus", ParDo.of(PrometheusReadFn(config)))
            .setRowSchema(PROMETHEUS_READ_SCHEMA)

        PAssert.that(output).satisfies { rows ->
            val rowList = rows.toList()
            assertEquals(2, rowList.size)
            val names = rowList.map { it.getString("metric_name") }.toSet()
            assertTrue(names.contains("up"))
            rowList.forEach { row ->
                assertEquals(1.0, row.getDouble("value")!!, 0.001)
            }
            null
        }
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `empty result returns no rows`() {
        val json = """
            {
              "status": "success",
              "data": {
                "resultType": "vector",
                "result": []
              }
            }
        """.trimIndent()

        val httpServer = startMockServer(json)
        val port = httpServer.address.port

        val config = PrometheusReadConfig(
            endpoint = "http://localhost:$port",
            query = "nonexistent_metric",
        )

        val pipeline = Pipeline.create()
        val output = pipeline
            .apply("Trigger", Create.of(listOf(1)))
            .apply("ReadFromPrometheus", ParDo.of(PrometheusReadFn(config)))
            .setRowSchema(PROMETHEUS_READ_SCHEMA)

        PAssert.that(output).empty()
        pipeline.run().waitUntilFinish()
    }
}
