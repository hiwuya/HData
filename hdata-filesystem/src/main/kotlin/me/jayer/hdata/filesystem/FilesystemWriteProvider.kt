package me.jayer.hdata.filesystem

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.filesystem.transform.FilesystemWriteFn
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

private class FilesystemSink(
    private val config: FilesystemWriteConfig,
    private val deadLetter: Boolean,
    private val transformName: String,
) : RowSink() {

    override fun write(input: PCollection<Row>): PCollection<Row>? {
        val inputSchema = input.schema
        val errorSchema = ErrorSchemas.of(inputSchema)
        val errors = input
            .apply("Write", ParDo.of(FilesystemWriteFn(config, inputSchema, errorSchema, deadLetter, transformName)))
            .setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
