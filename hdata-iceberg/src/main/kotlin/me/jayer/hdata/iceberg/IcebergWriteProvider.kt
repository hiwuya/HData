package me.jayer.hdata.iceberg

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.iceberg.transform.IcebergTruncateFn
import me.jayer.hdata.iceberg.transform.IcebergWriteFn
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.View
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `WriteToIceberg`：把行写入 Iceberg 表，支持死信输出。
 *
 * @author wuya
 */
class IcebergWriteProvider : TypedTransformProvider<IcebergWriteConfig>(IcebergWriteConfig::class.java) {

    override fun identifier(): String = "WriteToIceberg"

    override fun description(): String = "写入 Iceberg"

    override fun outputCollectionNames(): List<String> = listOf(me.jayer.hdata.core.spi.Tags.ERROR_OUTPUT)

    override fun create(config: IcebergWriteConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return IcebergSink(config, context.errorHandling != null, context.transformName)
    }
}

private class IcebergSink(
    private val config: IcebergWriteConfig,
    private val deadLetter: Boolean,
    private val transformName: String,
) : RowSink() {

    override fun write(input: PCollection<Row>): PCollection<Row>? {
        val errorSchema = ErrorSchemas.of(input.schema)
        var write = ParDo.of(IcebergWriteFn(config, errorSchema, deadLetter, transformName))
        if (config.mode() == IcebergWriteMode.OVERWRITE) {
            // 清表必须在所有写入之前恰好做一次。做成 side input：带 side input 的 ParDo
            // 在它算完之前不会处理任何主输入，这样"先清空再写"的顺序才有保证
            // （放进写入端的 @Setup 会让后一个 bundle 删掉前一个 bundle 刚写的数据）
            val truncated = input.pipeline
                .apply("TruncateTrigger", Create.of(""))
                .apply("Truncate", ParDo.of(IcebergTruncateFn(config)))
                .apply("TruncateDone", View.asList())
            write = write.withSideInputs(truncated)
        }
        val errors = input
            .apply("Write", write)
            .setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
