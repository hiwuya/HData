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

    @Transient
    private var csvFormat: CSVFormat? = null

    @Setup
    fun setup() {
        csvFormat = CSVFormat.DEFAULT.builder()
            .setDelimiter(config.csvDelimiter[0])
            .setQuote(config.csvQuote[0])
            .build()
    }

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
            val writer = StringWriter()
            checkNotNull(csvFormat).print(writer).use { it.printRecord(FilesystemSchemas.rowToFields(row, row.schema)) }
            // CSVPrinter 会自己补一个换行，TextIO.sink() 也会补，去掉重复的那个
            return writer.toString().trimEnd('\r', '\n')
        }
        require(row.schema.hasField(FilesystemSchemas.CONTENT_FIELD)) {
            "file_format=text 要求输入行有 ${FilesystemSchemas.CONTENT_FIELD}(STRING) 字段，现有字段: ${row.schema.fieldNames}"
        }
        return row.getString(FilesystemSchemas.CONTENT_FIELD)
            ?: throw IllegalArgumentException("${FilesystemSchemas.CONTENT_FIELD} 字段是 null，写不出一行文本")
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(RowToLineFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(RowToLineFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(RowToLineFn::class.java, "records_rejected")
    }
}
