package me.jayer.hdata.smoke

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.SourceMode
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TransformProvider
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.io.GenerateSequence
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.SimpleFunction
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * A purpose-built external plugin for Flink/Spark smoke jobs. It is intentionally unbounded so that
 * a runner must enter streaming mode, emit records on workers, and be cancelled by the smoke harness.
 */
class RunnerSmokeSourceProvider : TransformProvider {
    override fun identifier(): String = "ReadRunnerSmokeTicker"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun sourceMode(config: TransformConfig): SourceMode = SourceMode.UNBOUNDED

    override fun from(config: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> = RunnerSmokeSource()
}

class RunnerSmokeSource : RowSource() {
    override fun read(begin: PBegin): PCollection<Row> = begin
        .apply("Ticks", GenerateSequence.from(0))
        .apply("ToRows", MapElements.via(TickToRow))
        .setRowSchema(SCHEMA)

    private object TickToRow : SimpleFunction<Long, Row>() {
        override fun apply(input: Long): Row = Row.withSchema(SCHEMA).addValue(input).build()
    }

    private companion object {
        val SCHEMA: Schema = Schema.builder().addInt64Field("tick").build()
    }
}
