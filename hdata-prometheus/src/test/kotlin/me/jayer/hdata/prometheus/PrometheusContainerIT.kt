package me.jayer.hdata.prometheus

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import tools.jackson.databind.node.ObjectNode
import java.net.HttpURLConnection
import java.net.URL

/**
 * Covers the read path against a real Prometheus server: the container scrapes itself with its
 * default config, which is enough to produce the built-in `up` time series without needing a
 * separate exporter.
 *
 * Runs only under `-Pintegration-tests` (the `integration` tag excludes it from `mvn test`).
 */
@Tag("integration")
class PrometheusContainerIT {

    @Test
    fun `reads the built-in up metric from a real Prometheus server`() {
        GenericContainer<Nothing>(DockerImageName.parse("prom/prometheus:v2.55.1")).apply {
            withExposedPorts(HTTP_PORT)
            withStartupAttempts(3)
            waitingFor(Wait.forHttp("/-/ready").forStatusCode(200))
        }.use { prometheus ->
            prometheus.start()
            val endpoint = "http://${prometheus.host}:${prometheus.getMappedPort(HTTP_PORT)}"

            // The default image config scrapes "localhost:9090" under job "prometheus" every 15s;
            // wait for that first scrape to land before querying.
            waitForUpSeries(endpoint)

            val pipeline = Pipeline.create()
            val output = PCollectionRowTuple.empty(pipeline).apply(
                PrometheusReadProvider().from(
                    TransformConfig(
                        "ReadFromPrometheus",
                        SpecMappers.YAML.readTree(
                            """
                            endpoint: "$endpoint"
                            query: "up"
                            """.trimIndent(),
                        ) as ObjectNode,
                        null,
                    )
                )
            ).get(Tags.MAIN_OUTPUT)

            PAssert.that(output).satisfies { result ->
                val rows = result.toList()
                assert(rows.isNotEmpty()) { "Expected at least one 'up' series, got none" }
                val row = rows.first { it.getString("metric_name") == "up" }
                assert(row.getDouble("value") == 1.0) { "Expected up=1, got ${row.getDouble("value")}" }
                val labels = row.getMap<String, String>("labels")
                assert(labels?.get("job") == "prometheus") { "Expected job=prometheus, got: $labels" }
                null
            }
            pipeline.run().waitUntilFinish()
        }
    }

    /** Polls the instant-query API directly until the first scrape of "up" has landed. */
    private fun waitForUpSeries(endpoint: String) {
        val url = URL("$endpoint/api/v1/query?query=up")
        val maxAttempts = 30
        repeat(maxAttempts) { attempt ->
            val connection = url.openConnection() as HttpURLConnection
            try {
                connection.connectTimeout = 2_000
                connection.readTimeout = 2_000
                val body = connection.inputStream.bufferedReader().readText()
                if (body.contains("\"resultType\":\"vector\"") && body.contains("\"result\":[{")) return
            } catch (_: Exception) {
                // not ready yet
            } finally {
                connection.disconnect()
            }
            if (attempt < maxAttempts - 1) Thread.sleep(1_000)
        }
        throw RuntimeException("Prometheus did not scrape 'up' within $maxAttempts seconds")
    }

    private companion object {
        const val HTTP_PORT = 9090
    }
}
