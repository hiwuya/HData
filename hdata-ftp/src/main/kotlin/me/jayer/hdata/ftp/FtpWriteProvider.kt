package me.jayer.hdata.ftp

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.ftp.transform.FtpWriteFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `WriteToFtp`：把输入行逐行写成远程文件，支持死信输出。
 */
class FtpWriteProvider : TypedTransformProvider<FtpWriteConfig>(FtpWriteConfig::class.java) {

    override fun identifier(): String = "WriteToFtp"

    override fun description(): String = "把输入行逐行写成 FTP 远程文件，支持死信输出"

    override fun outputCollectionNames(): List<String> = listOf(me.jayer.hdata.core.spi.Tags.ERROR_OUTPUT)

    override fun create(
        config: FtpWriteConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return FtpSink(config, context.errorHandling != null, context.transformName)
    }
}

private class FtpSink(
    private val config: FtpWriteConfig,
    private val deadLetter: Boolean,
    private val transformName: String,
) : RowSink() {

    override fun write(input: PCollection<Row>): PCollection<Row>? {
        val inputSchema = input.schema
        val errorSchema = ErrorSchemas.of(inputSchema)
        val errors = input
            .apply(
                "Write",
                ParDo.of(
                    FtpWriteFn(
                        config.connection,
                        config.actualFileName,
                        config.fileFormat,
                        config.outputFieldNames,
                        config.encoding,
                        config.batchSize,
                        inputSchema,
                        errorSchema,
                        deadLetter,
                        transformName,
                    ),
                ),
            )
            .setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
