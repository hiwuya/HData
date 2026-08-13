package me.jayer.hdata.filesystem

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.filesystem.transform.FilesystemWriteFn
import me.jayer.hdata.filesystem.transform.RowBundle
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.coders.CoderRegistry
import org.apache.beam.sdk.coders.SerializableCoder
import org.apache.beam.sdk.transforms.Combine
import org.apache.beam.sdk.transforms.Combine.CombineFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `WriteToFilesystem`：批量写入文件系统，支持死信输出。
 */
class FilesystemWriteProvider : TypedTransformProvider<FilesystemWriteConfig>(FilesystemWriteConfig::class.java) {

    override fun identifier(): String = "WriteToFilesystem"

    override fun description(): String = "批量写入文件系统，支持死信输出"

    override fun outputCollectionNames(): List<String> = listOf(Tags.ERROR_OUTPUT)

    override fun create(
        config: FilesystemWriteConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return FilesystemSink(config, context.errorHandling != null, context.transformName)
    }
}

private const val SINK_KEY = "__filesystem_sink__"

private class FilesystemSink(
    private val config: FilesystemWriteConfig,
    private val deadLetter: Boolean,
    private val transformName: String,
) : RowSink() {

    override fun write(input: PCollection<Row>): PCollection<Row>? {
        val inputSchema = input.schema
        val errorSchema = ErrorSchemas.of(inputSchema)
        // Combine.globally 把全量行聚成单个 RowBundle，确保只由一个 DoFn 实例一次性写出整批，
        // 避免并行多实例各自截断同一输出文件（xlsx 尤其需要整本工作簿一次写完）。
        val combined: PCollection<RowBundle> = input.apply(
            "AccumulateAll",
            Combine.globally(AccumulateRows()).withoutDefaults(),
        )
        val errors = combined
            .apply("Write", ParDo.of(FilesystemWriteFn(config, inputSchema, errorSchema, deadLetter, transformName)))
            .setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * 把所有行累加进一个 [RowBundle]，供 `WriteToFilesystem` 在单个 DoFn 实例里整批写出。
 */
private class AccumulateRows : CombineFn<Row, RowBundle, RowBundle>() {

    override fun createAccumulator(): RowBundle = RowBundle(emptyList())

    override fun addInput(accumulator: RowBundle, row: Row): RowBundle =
        RowBundle(accumulator.rows + row)

    override fun mergeAccumulators(accumulators: MutableIterable<RowBundle>): RowBundle {
        val out = mutableListOf<Row>()
        accumulators.forEach { out.addAll(it.rows) }
        return RowBundle(out)
    }

    override fun extractOutput(accumulator: RowBundle): RowBundle = accumulator

    override fun getAccumulatorCoder(registry: CoderRegistry, inputCoder: Coder<Row>): Coder<RowBundle> =
        SerializableCoder.of(RowBundle::class.java)

    override fun getDefaultOutputCoder(registry: CoderRegistry, inputCoder: Coder<Row>): Coder<RowBundle> =
        SerializableCoder.of(RowBundle::class.java)
}
