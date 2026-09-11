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
 * `WriteToFilesystem`: write files, persisting to disk via Beam's `FileIO.write()`.
 *
 * Before the refactor this approach had two fatal problems:
 *
 *  1. It used `Combine.globally` to aggregate the **whole dataset** into a single `RowBundle` and then
 *     handed it to one DoFn to write out. This did avoid multiple instances truncating the same file
 *     at the same time, but at the cost of loading all data into a single machine's memory and writing
 *     to disk on a single thread — which largely defeats the purpose of using Beam.
 *  2. The file was opened in `@Setup` via `create(path, overwrite = true)`. Beam does not guarantee that
 *     `@Setup`/`@Teardown` run exactly once per worker, so on job retry the already-written result would
 *     be truncated directly.
 *
 * `FileIO.write()` solves exactly this class of problem: shards write their own temp files in parallel,
 * and only after all succeed are they atomically renamed into place.
 *
 * @author wuya
 */
class FilesystemWriteProvider : TypedTransformProvider<FilesystemWriteConfig>(FilesystemWriteConfig::class.java) {

    override fun identifier(): String = "WriteToFilesystem"

    override fun description(): String = "Write to the filesystem, reusing Beam's FileIO.write() to shard the output, with dead-letter output support"

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
            // xlsx writes the whole workbook at once, so there is no such thing as "a single row write failed"
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
