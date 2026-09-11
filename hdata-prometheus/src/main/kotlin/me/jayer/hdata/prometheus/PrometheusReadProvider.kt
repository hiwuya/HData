package me.jayer.hdata.prometheus

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.prometheus.transform.PrometheusReadFn
import me.jayer.hdata.prometheus.transform.PROMETHEUS_READ_SCHEMA
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `ReadFromPrometheus`: executes a PromQL instant query and returns one row per time series.
 *
 * @author wuya
 */
class PrometheusReadProvider : TypedTransformProvider<PrometheusReadConfig>(PrometheusReadConfig::class.java) {

    override fun identifier(): String = "ReadFromPrometheus"

    override fun description(): String = "Read from Prometheus via a PromQL instant query"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(
        config: PrometheusReadConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return PrometheusSource(config)
    }
}

private class PrometheusSource(private val config: PrometheusReadConfig) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        return begin
            .apply("Trigger", Create.of(listOf(1)))
            .apply("ReadFromPrometheus", ParDo.of(PrometheusReadFn(config)))
            .setRowSchema(PROMETHEUS_READ_SCHEMA)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
