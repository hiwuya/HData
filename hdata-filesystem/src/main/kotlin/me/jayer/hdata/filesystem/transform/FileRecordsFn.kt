package me.jayer.hdata.filesystem.transform

import me.jayer.hdata.filesystem.FilesystemReadConfig
import me.jayer.hdata.filesystem.FilesystemSchemas
import me.jayer.hdata.filesystem.RecordParser
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.apache.commons.csv.CSVFormat
import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.ss.usermodel.DateUtil
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.slf4j.LoggerFactory
import java.nio.channels.Channels
import java.nio.charset.Charset

/**
 * 整文件读取 `csv` / `xlsx`。
 *
 * 这两种格式**不能**按字节区间切分，所以这里是一个文件一个处理单元：
 *  - CSV 的字段可以带引号并在引号内换行，从任意字节位置切开会把一条记录劈成两半；
 *  - xlsx 是一个 zip 容器，只有从头解析才有意义。
 *
 * 并行度因此来自"文件个数"而不是"文件内部"。`text` 格式没有这个限制，走的是
 * Beam 的 `TextIO.readFiles()`，那才是真正按字节区间切分的 Splittable DoFn。
 *
 * @author wuya
 */
class FileRecordsFn(
    private val config: FilesystemReadConfig,
    private val schema: Schema,
) : DoFn<FileIO.ReadableFile, Row>() {

    @Transient
    private var parser: RecordParser? = null

    @Setup
    fun setup() {
        parser = RecordParser(schema)
    }

    @ProcessElement
    fun processElement(@Element file: FileIO.ReadableFile, receiver: OutputReceiver<Row>) {
        val name = file.metadata.resourceId().toString()
        val count = when (config.fileFormat) {
            FilesystemReadConfig.XLSX -> readXlsx(file, name, receiver)
            else -> readCsv(file, name, receiver)
        }
        RECORDS_READ.inc(count)
        LOGGER.info("文件[{}] 读出 {} 行", name, count)
    }

    private fun readCsv(file: FileIO.ReadableFile, name: String, receiver: OutputReceiver<Row>): Long {
        val format = CSVFormat.DEFAULT.builder()
            .setDelimiter(config.csvDelimiter[0])
            .setQuote(config.csvQuote[0])
            .setIgnoreEmptyLines(true)
            .build()
        var count = 0L
        Channels.newInputStream(file.open()).bufferedReader(Charset.forName(config.encoding)).use { reader ->
            val iterator = format.parse(reader).iterator()
            if (config.header && iterator.hasNext()) {
                iterator.next()
            }
            for (record in iterator) {
                receiver.output(checkNotNull(parser).parse(record.toList(), name, record.recordNumber))
                count++
            }
        }
        return count
    }

    private fun readXlsx(file: FileIO.ReadableFile, name: String, receiver: OutputReceiver<Row>): Long {
        var count = 0L
        Channels.newInputStream(file.open()).use { stream ->
            XSSFWorkbook(stream).use { workbook ->
                val sheet = config.sheet.takeIf { it.isNotBlank() }?.let {
                    workbook.getSheet(it) ?: throw IllegalArgumentException("文件[$name] 里没有名为 $it 的工作表")
                } ?: workbook.getSheetAt(0)
                val rows = sheet.iterator()
                if (config.header && rows.hasNext()) {
                    rows.next()
                }
                while (rows.hasNext()) {
                    val row = rows.next() ?: continue
                    val fields = schema.fields.indices.map { cellText(row.getCell(it)) }
                    receiver.output(checkNotNull(parser).parse(fields, name, row.rowNum + 1L))
                    count++
                }
            }
        }
        return count
    }

    private fun cellText(cell: Cell?): String? {
        if (cell == null) return null
        return when (cell.cellType) {
            CellType.BLANK -> null
            CellType.BOOLEAN -> cell.booleanCellValue.toString()
            CellType.NUMERIC ->
                if (DateUtil.isCellDateFormatted(cell)) {
                    cell.localDateTimeCellValue.toString()
                } else {
                    // 整数别写成 1.0：下游按 int 解析会直接失败
                    val d = cell.numericCellValue
                    if (d == d.toLong().toDouble()) d.toLong().toString() else d.toString()
                }
            CellType.STRING -> cell.stringCellValue
            CellType.FORMULA -> runCatching { cell.stringCellValue }.getOrElse { cell.numericCellValue.toString() }
            else -> null
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(FileRecordsFn::class.java)
        private val RECORDS_READ = Metrics.counter(FileRecordsFn::class.java, "records_read")
    }
}

/** `text` 格式：`TextIO.readFiles()` 读出的每一行包成单列 [Row]。 */
class TextLineToRowFn : DoFn<String, Row>() {

    @ProcessElement
    fun processElement(@Element line: String, receiver: OutputReceiver<Row>) {
        RECORDS_READ.inc()
        receiver.output(Row.withSchema(FilesystemSchemas.TEXT_SCHEMA).addValue(line).build())
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val RECORDS_READ = Metrics.counter(TextLineToRowFn::class.java, "records_read")
    }
}
