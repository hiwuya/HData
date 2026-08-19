package me.jayer.hdata.filesystem

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.filesystem.transform.RowToLineFn
import me.jayer.hdata.filesystem.transform.EncodedTextSink
import me.jayer.hdata.filesystem.transform.XlsxSink
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.PCollectionTuple
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.TupleTag
import org.apache.beam.sdk.values.TupleTagList

/**
 * `WriteToFilesystem`：写文件，落盘复用 Beam 的 `FileIO.write()`。
 *
 * 重构前这里的写法有两个致命问题：
 *
 *  1. 用 `Combine.globally` 把**整个数据集**聚成一个 `RowBundle` 再交给单个 DoFn 写出。
 *     这确实避免了多个实例同时截断同一个文件，但代价是全量数据进单机内存、单线程落盘——
 *     用 Beam 的意义基本被抵消了。
 *  2. 文件在 `@Setup` 里 `create(path, overwrite = true)` 打开。Beam 并不保证
 *     `@Setup`/`@Teardown` 每个 worker 只走一次，作业重试时会把已经写好的结果直接截断。
 *
 * `FileIO.write()` 解决的正是这一类问题：分片并行写各自的临时文件，全部成功后才原子改名到位。
 *
 * @author wuya
 */
class FilesystemWriteProvider : TypedTransformProvider<FilesystemWriteConfig>(FilesystemWriteConfig::class.java) {

    override fun identifier(): String = "WriteToFilesystem"

    override fun description(): String = "写入文件系统，复用 Beam 的 FileIO.write() 分片写出，支持死信输出"

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
        val errorSchema = ErrorSchemas.of(input.schema)
        if (config.fileFormat == FilesystemReadConfig.XLSX) {
            writeXlsx(input)
            // xlsx 是整本工作簿一次写出，没有"单条写失败"这回事
            return if (deadLetter) emptyErrors(input, errorSchema) else null
        }

        val errorTag = TupleTag<Row>()
        val mainTag = object : TupleTag<String>() {}
        val outputs: PCollectionTuple = input.apply(
            "ToLines",
            ParDo.of(RowToLineFn(config, errorSchema, deadLetter, transformName, errorTag))
                .withOutputTags(mainTag, TupleTagList.of(errorTag)),
        )

        outputs.get(mainTag).apply("WriteFiles", textWrite())

        val errors = outputs.get(errorTag).setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }

    private fun textWrite(): FileIO.Write<Void, String> {
        val header = if (config.header && config.fileFormat == FilesystemReadConfig.CSV) {
            FilesystemSchemas.csvRecord(
                FilesystemSchemas.csvHeader(FilesystemSchemas.build(config)),
                config.csvDelimiter[0],
                config.csvQuote[0],
            )
        } else null
        return FileIO.write<String>()
            .via(EncodedTextSink(config.encoding, header))
            .to(FilesystemPaths.normalize(config.path, config.defaultFs))
            .withPrefix(config.filePrefix)
            .withSuffix(config.suffix())
            .let { if (config.numShards > 0) it.withNumShards(config.numShards) else it }
    }

    private fun writeXlsx(input: PCollection<Row>) {
        input.apply(
            "WriteXlsx",
            FileIO.write<Row>()
                .via(XlsxSink(config, FilesystemSchemas.build(config)))
                .to(FilesystemPaths.normalize(config.path, config.defaultFs))
                .withPrefix(config.filePrefix)
                .withSuffix(config.suffix())
                .withNumShards(1),
        )
    }

    private fun emptyErrors(input: PCollection<Row>, errorSchema: org.apache.beam.sdk.schemas.Schema): PCollection<Row> =
        input.pipeline.apply("NoErrors", Create.empty(errorSchema)).setRowSchema(errorSchema)

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
