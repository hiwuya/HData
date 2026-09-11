package me.jayer.hdata.prometheus.transform

import me.jayer.hdata.prometheus.PrometheusReadConfig
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory
import tools.jackson.databind.JsonNode
import tools.jackson.databind.ObjectMapper
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder

/**
 * Executes a PromQL instant query against the Prometheus HTTP API and converts each time series
 * into a Beam [Row].
 *
 * @author wuya
 */
class PrometheusReadFn(
    private val config: PrometheusReadConfig,
) : DoFn<Any, Row>() {

    @Transient
    private var mapper: ObjectMapper? = null

    @Setup
    fun setup() {
        mapper = ObjectMapper()
    }

    @ProcessElement
    fun processElement(context: ProcessContext) {
        val url = buildQueryUrl()
        LOGGER.info("Executing Prometheus query: {}", config.query)

        val connection = URL(url).openConnection() as HttpURLConnection
        connection.connectTimeout = config.connectTimeoutMs
        connection.readTimeout = config.readTimeoutMs
        connection.setRequestProperty("Accept", "application/json")

        try {
            if (connection.responseCode != 200) {
                val errorBody = connection.errorStream?.bufferedReader()?.readText() ?: ""
                throw RuntimeException("Prometheus query failed with HTTP ${connection.responseCode}: $errorBody")
            }

            val json = checkNotNull(mapper).readTree(InputStreamReader(connection.inputStream, Charsets.UTF_8))
            val status = json.get("status")?.asString()
            if (status != "success") {
                val errorType = json.get("errorType")?.asString() ?: "unknown"
                val error = json.get("error")?.asString() ?: "no message"
                throw RuntimeException("Prometheus query error [$errorType]: $error")
            }

            val data = json.get("data")
            val resultType = data?.get("resultType")?.asString()
            if (resultType != "vector") {
                LOGGER.warn("Unexpected resultType: {}; expected 'vector'", resultType)
                return
            }

            val result = data.get("result")
            if (result == null || !result.isArray) return

            for (series in result) {
                val metric = series.get("metric")
                val value = series.get("value")

                if (metric == null || value == null || !value.isArray || value.size() < 2) continue

                // Extract __name__ as metric_name and remove it from labels.
                val metricName = metric.get("__name__")?.asString() ?: ""

                val labels = mutableMapOf<String, String>()
                for (fieldName in metric.propertyNames()) {
                    if (fieldName != "__name__") {
                        labels[fieldName] = metric.get(fieldName).asString()
                    }
                }

                val tsValue = value.get(1).asDouble()
                val tsTimestamp = value.get(0).asDouble()

                val row = Row.withSchema(PROMETHEUS_READ_SCHEMA)
                    .addValue(metricName)
                    .addValue(labels)
                    .addValue(tsValue)
                    .addValue(tsTimestamp)
                    .build()
                context.output(row)
            }
        } finally {
            connection.disconnect()
        }
    }

    private fun buildQueryUrl(): String {
        val base = config.endpoint.trimEnd('/')
        val encodedQuery = URLEncoder.encode(config.query, "UTF-8")
        val url = "$base/api/v1/query?query=$encodedQuery"
        return if (config.time.isNotBlank()) {
            "$url&time=${URLEncoder.encode(config.time, "UTF-8")}"
        } else {
            url
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(PrometheusReadFn::class.java)
    }
}
