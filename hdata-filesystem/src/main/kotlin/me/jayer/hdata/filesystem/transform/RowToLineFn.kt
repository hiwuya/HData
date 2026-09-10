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
 * 把 [Row] 转成要写出的一行文本，转不了的送去死信。
 *
 * 把转换从写文件里拆出来，是为了让 `WriteToFilesystem` 能直接用 Beam 的 `FileIO.write()`：
 * 后者负责分片、临时文件与原子改名，但它只接受"已经是最终形态"的元素，
 * 也不提供逐条的错误出口。转换恰恰是写文件这条链上唯一会因**单条数据**失败的环节，
 * 放在这里既保住了死信能力，又能把落盘交给 Beam。
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
     * `schema_fields` 解析出来的 schema，以及 CSV 的写出格式，都只算一次。
     *
     * 重构前 `format()` 每处理**一行**就 `FilesystemSchemas.build(config)` 一遍（split + 校验 + 建 Schema），
     * 还要现造一个 `CSVFormat`——这是逐行热路径上的纯浪费，与 `MongoRowCodec` 当初拆出来的原因一样。
     *
     * 惰性初始化而不是放进 `@Setup`，是为了让直接调 `processElement` 的单测（见 `RowToLineFnTest`）
     * 不必先跑一遍 lifecycle。
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
            LOGGER.warn("转换写出行失败，转入死信: {}", e.message)
            RECORDS_REJECTED.inc()
            context.output(errorTag, ErrorSchemas.failure(errorSchema, row, e, transformName))
        }
    }

    private fun format(row: Row): String {
        if (config.fileFormat == FilesystemReadConfig.CSV) {
            return csvRecord(FilesystemSchemas.rowToFields(row, resolvedSchema()))
        }
        require(row.schema.hasField(FilesystemSchemas.CONTENT_FIELD)) {
            "file_format=text 要求输入行有 ${FilesystemSchemas.CONTENT_FIELD}(STRING) 字段，现有字段: ${row.schema.fieldNames}"
        }
        return row.getString(FilesystemSchemas.CONTENT_FIELD)
            ?: throw IllegalArgumentException("${FilesystemSchemas.CONTENT_FIELD} 字段是 null，写不出一行文本")
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
