package me.jayer.hdata.filesystem.transform

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.filesystem.FilesystemReadConfig
import me.jayer.hdata.filesystem.FilesystemSchemas
import me.jayer.hdata.filesystem.FilesystemWriteConfig
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.TupleTag
import org.apache.commons.csv.CSVFormat
import org.slf4j.LoggerFactory
import java.io.StringWriter

/**
 * Turns a [Row] into the line of text to write out; rows that cannot be converted go to the dead letter.
 *
 * Conversion is split out of the file writing so that `WriteToFilesystem` can use Beam's
 * `FileIO.write()` directly: the latter owns sharding, temp files and the atomic rename, but it only
 * accepts elements that are *already in their final form* and offers no per-record error output.
 * Conversion is precisely the only step in the write chain that can fail because of a **single
 * record**, so keeping it here preserves dead letter support while leaving the actual write to Beam.
 *
 * @author wuya
 */
class RowToLineFn(
    private val config: FilesystemWriteConfig,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
    private val errorTag: TupleTag<Row>,
) : DoFn<Row, String>() {

    /**
     * The schema parsed from `schema_fields` and the CSV output format are both computed only once.
     *
     * Before the refactor `format()` ran `FilesystemSchemas.build(config)` for **every single row**
     * (split + validation + Schema building) and also built a fresh `CSVFormat` -- pure waste on a
     * per-row hot path, the same reason `MongoRowCodec` was factored out back then.
     *
     * It is lazily initialized rather than built in `@Setup` so that unit tests calling
     * `processElement` directly (see `RowToLineFnTest`) do not have to run the lifecycle first.
     */
    @Transient
    private var schema: Schema? = null

    @Transient
    private var csvFormat: CSVFormat? = null

    private fun resolvedSchema(): Schema = schema ?: FilesystemSchemas.build(config).also { schema = it }

    private fun resolvedCsvFormat(): CSVFormat = csvFormat ?: CSVFormat.DEFAULT.builder()
        .setDelimiter(config.csvDelimiter[0])
        .setQuote(config.csvQuote[0])
        .get()
        .also { csvFormat = it }

    @ProcessElement
    fun processElement(@Element row: Row, context: ProcessContext) {
        try {
            context.output(format(row))
            RECORDS_WRITTEN.inc()
        } catch (e: Exception) {
            if (!deadLetter) {
                throw e
            }
            LOGGER.warn("failed to convert an output row, sending it to the dead letter: {}", e.message)
            RECORDS_REJECTED.inc()
            context.output(errorTag, ErrorSchemas.failure(errorSchema, row, e, transformName))
        }
    }

    private fun format(row: Row): String {
        if (config.fileFormat == FilesystemReadConfig.CSV) {
            return csvRecord(FilesystemSchemas.rowToFields(row, resolvedSchema()))
        }
        require(row.schema.hasField(FilesystemSchemas.CONTENT_FIELD)) {
            "file_format=text requires the input row to have a ${FilesystemSchemas.CONTENT_FIELD}(STRING) field; existing fields: ${row.schema.fieldNames}"
        }
        return row.getString(FilesystemSchemas.CONTENT_FIELD)
            ?: throw IllegalArgumentException("${FilesystemSchemas.CONTENT_FIELD} is null, so no line of text can be written")
    }

    private fun csvRecord(fields: List<Any?>): String {
        val writer = StringWriter()
        resolvedCsvFormat().print(writer).use { it.printRecord(fields) }
        return writer.toString().trimEnd('\r', '\n')
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(RowToLineFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(RowToLineFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(RowToLineFn::class.java, "records_rejected")
    }
}
