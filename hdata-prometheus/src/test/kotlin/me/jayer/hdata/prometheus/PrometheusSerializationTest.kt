package me.jayer.hdata.prometheus

import me.jayer.hdata.prometheus.transform.PrometheusReadFn
import org.apache.beam.sdk.util.SerializableUtils
import org.junit.jupiter.api.Test

/**
 * A DoFn that captures a non-serializable object only blows up when the job is submitted; a unit
 * test that only calls `processElement` would never catch it, so every connector has at least one
 * `ensureSerializable` assertion.
 */
class PrometheusSerializationTest {

    @Test
    fun `DoFns are serializable`() {
        SerializableUtils.ensureSerializable(
            PrometheusReadFn(PrometheusReadConfig(endpoint = "http://localhost:9090", query = "up"))
        )
    }
}
