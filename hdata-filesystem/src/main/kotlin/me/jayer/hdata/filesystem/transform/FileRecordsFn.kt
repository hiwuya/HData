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
 * Whole-file reading of `csv` / `xlsx`.
 *
 * Neither format **can** be split by byte range, so here one file is one unit of work:
 *  - CSV fields can be quoted and contain newlines inside the quotes, so cutting at an arbitrary
 *    byte position would split a record in half;
 *  - xlsx is a zip container that only makes sense to parse from the beginning.
 *
 * Parallelism therefore comes from the *number of files* rather than from *inside a file*. The
 * `text` format has no such limitation: it goes through Beam's `TextIO.readFiles()`, which is the
 * real Splittable DoFn that splits by byte range.
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
        LOGGER.info("file[{}] read {} rows", name, count)
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
                    workbook.getSheet(it) ?: throw IllegalArgumentException("file[$name] has no sheet named $it")
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
            CellType.NUMERIC -> numericCellText(cell)
            CellType.STRING -> cell.stringCellValue
            // a formula cell's own type is always FORMULA, so we have to look at the cached result type;
            // the old implementation tried string first and then always fell back to numeric, which made
            // BOOLEAN formulas throw IllegalStateException.
            CellType.FORMULA -> when (cell.cachedFormulaResultType) {
                CellType.BLANK -> null
                CellType.BOOLEAN -> cell.booleanCellValue.toString()
                CellType.NUMERIC -> numericCellText(cell)
                CellType.STRING -> cell.stringCellValue
                CellType.ERROR -> throw IllegalArgumentException(
                    "the cached result of Excel formula [${cell.cellFormula}] is error code ${cell.errorCellValue}"
                )
                else -> null
            }
            CellType.ERROR -> throw IllegalArgumentException("Excel cell holds error code ${cell.errorCellValue}")
            else -> null
        }
    }

    private fun numericCellText(cell: Cell): String =
        if (DateUtil.isCellDateFormatted(cell)) {
            cell.localDateTimeCellValue.toString()
        } else {
            // do not write integers as 1.0: downstream int parsing would fail outright
            val value = cell.numericCellValue
            if (value == value.toLong().toDouble()) value.toLong().toString() else value.toString()
        }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(FileRecordsFn::class.java)
        private val RECORDS_READ = Metrics.counter(FileRecordsFn::class.java, "records_read")
    }
}

/** `text` format: wraps each line read by `TextIO.readFiles()` into a single-column [Row]. */
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
