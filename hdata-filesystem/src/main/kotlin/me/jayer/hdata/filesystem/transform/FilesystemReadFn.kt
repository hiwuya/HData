package me.jayer.hdata.filesystem.transform

import me.jayer.hdata.filesystem.FilesystemReadConfig
import me.jayer.hdata.filesystem.FilesystemSchemas
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.values.Row
import org.apache.commons.csv.CSVFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.ss.usermodel.DateUtil
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.slf4j.LoggerFactory
import java.net.URI
import java.nio.charset.Charset

/**
 * 按文件切分并行读文件系统：元素是一个文件路径（String），限制用 `OffsetRange(0,1)` 表示"整个文件一段"，
 * 交给 Beam 的 splittable DoFn 框架处理（每个文件一个 element 一次处理）。
 *
 * `@ProcessElement` 按 `file_format` 分发：
 *  - `text`：逐行读出，每行一个 `content` 字段；
 *  - `csv`：用 Commons CSV(RFC4180) 解析，支持引号/转义/内嵌换行，`header=true` 跳过首行；
 *  - `xlsx`：用 POI 打开工作簿，按 `schema_fields` 顺序读单元格，`header=true` 跳过首行。
 */
@DoFn.BoundedPerElement
class FilesystemReadFn(
    private val config: FilesystemReadConfig,
    private val schema: Schema,
) : DoFn<String, Row>() {

    @Transient
    private var fs: FileSystem? = null

    @Setup
    fun setup() {
        fs = FileSystem.get(URI(config.defaultFs), newConf())
    }

    @Teardown
    fun tearDown() {
        // 不要关闭通过 FileSystem.get 取得的共享（缓存）实例，否则会影响复用同一本地 fs 的其他 transform。
        fs = null
    }

    @GetInitialRestriction
    fun getInitialRestriction(@Element path: String): OffsetRange = OffsetRange(0, 1)

    @SplitRestriction
    fun splitRestriction(
        @Element path: String,
        @Restriction restriction: OffsetRange,
        receiver: OutputReceiver<OffsetRange>,
    ) {
        receiver.output(restriction)
    }

    @ProcessElement
    fun processElement(
        @Element path: String,
        tracker: RestrictionTracker<OffsetRange, Long>,
        receiver: OutputReceiver<Row>,
    ) {
        val range = tracker.currentRestriction()
        if (range.to <= range.from) {
            return
        }
        if (!tracker.tryClaim(range.to - 1)) {
            return
        }
        val fileSystem = checkNotNull(fs) { "FileSystem 未初始化" }
        var count = 0L
        open(path).use { inStream ->
            count = when (config.fileFormat) {
                "csv" -> readCsv(inStream, receiver)
                "xlsx" -> readXlsx(inStream, receiver)
                else -> readText(inStream, receiver)
            }
        }
        RECORDS_READ.inc(count)
        LOGGER.info("文件[{}] 读完 {} 行", path, count)
    }

    /**
     * 打开文件输入流。`file://` 走本地 [java.io.FileInputStream]（绕开 Hadoop 本地 checksum 校验，
     * 避免二进制文件如 xlsx 在读回时偶发的 ChecksumException）；其余 scheme（如 hdfs）走 Hadoop FileSystem。
     */
    private fun open(path: String): java.io.InputStream {
        val p = Path(path)
        return if (p.toUri().scheme == "file" || config.defaultFs.startsWith("file")) {
            java.io.FileInputStream(p.toUri().path)
        } else {
            checkNotNull(fs).open(p)
        }
    }

    private fun readText(inStream: java.io.InputStream, receiver: OutputReceiver<Row>): Long {
        val charset = Charset.forName(config.encoding)
        var count = 0L
        inStream.bufferedReader(charset).use { reader ->
            var line: String?
            while (reader.readLine().also { line = it } != null) {
                val l = line ?: continue
                receiver.output(Row.withSchema(schema).addValue(l).build())
                count++
            }
        }
        return count
    }

    private fun readCsv(inStream: java.io.InputStream, receiver: OutputReceiver<Row>): Long {
        val charset = Charset.forName(config.encoding)
        var count = 0L
        inStream.bufferedReader(charset).use { reader ->
            val parser = CSVFormat.DEFAULT.builder().setIgnoreEmptyLines(true).build().parse(reader)
            val iterator = parser.iterator()
            if (config.header && iterator.hasNext()) {
                iterator.next()
            }
            for (record in iterator) {
                receiver.output(FilesystemSchemas.rowFromFields(record.map { it }, schema))
                count++
            }
        }
        return count
    }

    private fun readXlsx(inStream: java.io.InputStream, receiver: OutputReceiver<Row>): Long {
        var count = 0L
        XSSFWorkbook(inStream).use { workbook ->
            val sheet = if (config.sheet.isNotBlank()) {
                workbook.getSheet(config.sheet) ?: workbook.getSheetAt(0)
            } else {
                workbook.getSheetAt(0)
            }
            val rows = sheet.iterator()
            if (config.header && rows.hasNext()) {
                rows.next()
            }
            while (rows.hasNext()) {
                val row = rows.next() ?: continue
                val fields = schema.fields.mapIndexed { index, _ ->
                    cellRawValue(row.getCell(index))
                }
                receiver.output(FilesystemSchemas.rowFromFields(fields, schema))
                count++
            }
        }
        return count
    }

    private fun cellRawValue(cell: Cell?): String? {
        if (cell == null) return null
        return when (cell.cellType) {
            CellType.BLANK -> null
            CellType.BOOLEAN -> cell.booleanCellValue.toString()
            CellType.NUMERIC -> {
                if (DateUtil.isCellDateFormatted(cell)) {
                    cell.dateCellValue.toString()
                } else {
                    val d = cell.numericCellValue
                    if (d == d.toLong().toDouble()) d.toLong().toString() else d.toString()
                }
            }
            CellType.STRING -> cell.stringCellValue
            CellType.FORMULA -> cell.stringCellValue
            else -> null
        }
    }

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = restriction.newTracker()

    @GetRestrictionCoder
    fun restrictionCoder(): Coder<OffsetRange> = OffsetRange.Coder()

    private fun newConf(): Configuration = Configuration().apply {
        this["fs.defaultFS"] = config.defaultFs
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(FilesystemReadFn::class.java)
        private val RECORDS_READ = Metrics.counter(FilesystemReadFn::class.java, "records_read")
    }
}
