package me.jayer.hdata.filesystem.transform

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.filesystem.FilesystemSchemas
import me.jayer.hdata.filesystem.FilesystemWriteConfig
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.slf4j.LoggerFactory
import java.io.Serializable
import java.net.URI
import java.nio.charset.Charset

/**
 * `Combine.globally` 输出用的包裹类：把一批 [Row] 打包成一个具体类型，
 * 确保下游 `FilesystemWriteFn` 只有单个 DoFn 实例、一次性写出整批（尤其是 xlsx 需要整本工作簿一次写完）。
 * 用具体类而非 `List<Row>` 是为了绕过 Kotlin 在父类类型参数里会丢掉 `out` 投影、导致 Beam
 * `DoFn<List<Row>, Row>` 校验失败（DoFn 签名里 `List<Row>` 与 PCollection 的 `List<? extends Row>` 不匹配）的问题。
 */
class RowBundle(val rows: List<Row>) : Serializable {
    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * 写入文件系统。`text`/`csv` 走流式攒批写出（`csv` 用 Commons CSV 正确转义，支持 `header` 输出表头）；
 * `xlsx` 在 `@Teardown` 一次性把累积的全部行写成一个工作簿（按 `schema_fields` 顺序映射列，
 * 数值/布尔写成本类型单元格，`header=true` 时先写表头行）。
 *
 * 写失败且开了死信时，失败行经 `ErrorSchemas.failure(...)` 进死信流；没开死信时异常直接抛出，作业失败。
 */
class FilesystemWriteFn(
    private val config: FilesystemWriteConfig,
    private val inputSchema: Schema,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
) : DoFn<RowBundle, Row>() {

    @Transient
    private var fs: FileSystem? = null

    @Transient
    private var writer: java.io.BufferedWriter? = null

    @Transient
    private var csvPrinter: CSVPrinter? = null

    @Transient
    private var xlsxStream: org.apache.hadoop.fs.FSDataOutputStream? = null

    /** xlsx 模式：累积的全部行，在 teardown 一次性写出。 */
    private val xlsxRows = mutableListOf<Row>()

    private val buffered = mutableListOf<ValueInSingleWindow<Row>>()
    private val failures = mutableListOf<ValueInSingleWindow<Row>>()

    @Setup
    fun setup() {
        val fileSystem = FileSystem.get(URI(config.defaultFs), newConf())
        fs = fileSystem
        val out = fileSystem.create(Path(outputPath()), true)
        when (config.fileFormat) {
            "xlsx" -> xlsxStream = out
            "csv" -> {
                writer = out.bufferedWriter(Charset.forName(config.encoding))
                csvPrinter = CSVPrinter(
                    writer,
                    CSVFormat.DEFAULT.builder().setIgnoreEmptyLines(false).build(),
                )
                if (config.header) {
                    csvPrinter!!.printRecord(FilesystemSchemas.csvHeader(inputSchema))
                }
            }
            else -> writer = out.bufferedWriter(Charset.forName(config.encoding))
        }
    }

    @StartBundle
    fun startBundle() {
        buffered.clear()
        failures.clear()
    }

    @ProcessElement
    fun processElement(
        @Element bundle: RowBundle,
        @Timestamp timestamp: org.joda.time.Instant,
        window: BoundedWindow,
        pane: PaneInfo,
    ) {
        bundle.rows.forEach { row ->
            val visw = ValueInSingleWindow.of(row, timestamp, window, pane)
            if (config.fileFormat == "xlsx") {
                try {
                    FilesystemSchemas.rowToFields(row, inputSchema)
                    xlsxRows.add(row)
                } catch (e: Exception) {
                    routeDeadLetter(visw, e)
                }
            } else {
                buffered.add(visw)
            }
        }
        if (config.fileFormat != "xlsx" && buffered.size >= config.batchSize) {
            flush()
        }
    }

    @FinishBundle
    fun finishBundle(context: FinishBundleContext) {
        flush()
        failures.forEach { context.output(it.value, it.timestamp, it.window) }
        failures.clear()
    }

    @Teardown
    fun tearDown() {
        if (config.fileFormat == "xlsx") {
            runCatching { writeXlsx(xlsxRows, checkNotNull(xlsxStream)) }
            runCatching { xlsxStream?.close() }
            xlsxStream = null
        } else {
            runCatching { writer?.flush() }
            runCatching { writer?.close() }
        }
        writer = null
        csvPrinter = null
        // 不关闭通过 FileSystem.get 取得的共享（缓存）实例，否则会影响复用同一本地 fs 的其他 transform。
        fs = null
    }

    private fun flush() {
        if (buffered.isEmpty()) {
            return
        }
        val w = checkNotNull(writer) { "写入流未初始化" }
        buffered.forEach { record ->
            try {
                when (config.fileFormat) {
                    "csv" -> csvPrinter!!.printRecord(FilesystemSchemas.rowToFields(record.value, inputSchema))
                    else -> {
                        w.write(formatTextRow(record.value))
                        w.newLine()
                    }
                }
                RECORDS_WRITTEN.inc()
            } catch (e: Exception) {
                routeDeadLetter(record, e)
            }
        }
        runCatching { w.flush() }
        buffered.clear()
    }

    private fun routeDeadLetter(record: ValueInSingleWindow<Row>, e: Exception) {
        if (!deadLetter) {
            throw e
        }
        LOGGER.warn("写入文件系统失败，转入死信: {}", e.message)
        RECORDS_REJECTED.inc()
        failures.add(
            ValueInSingleWindow.of(
                ErrorSchemas.failure(errorSchema, record.value, e, transformName),
                record.timestamp,
                record.window,
                record.paneInfo,
            )
        )
    }

    private fun writeXlsx(rows: List<Row>, stream: org.apache.hadoop.fs.FSDataOutputStream) {
        XSSFWorkbook().use { workbook ->
            val sheet = workbook.createSheet(config.sheet.ifBlank { "Sheet1" })
            var r = 0
            if (config.header) {
                val header = sheet.createRow(r++)
                inputSchema.fields.forEachIndexed { index, field ->
                    header.createCell(index).setCellValue(field.name)
                }
            }
            rows.forEach { row ->
                val xrow = sheet.createRow(r++)
                inputSchema.fields.forEachIndexed { index, field ->
                    setCell(xrow.createCell(index), row.getValue<Any?>(index), field.type)
                }
                RECORDS_WRITTEN.inc()
            }
            workbook.write(stream)
        }
    }

    private fun setCell(cell: org.apache.poi.ss.usermodel.Cell, value: Any?, type: Schema.FieldType) {
        if (value == null) {
            cell.setBlank()
            return
        }
        when (type.typeName) {
            Schema.TypeName.INT32,
            Schema.TypeName.INT64,
            Schema.TypeName.INT16,
            Schema.TypeName.BYTE,
            Schema.TypeName.FLOAT,
            Schema.TypeName.DOUBLE -> cell.setCellValue((value as Number).toDouble())

            Schema.TypeName.BOOLEAN -> cell.setCellValue(value as Boolean)
            else -> cell.setCellValue(value.toString())
        }
    }

    private fun formatTextRow(row: Row): String {
        val v: Any? = row.getValue<Any?>(FilesystemSchemas.CONTENT_FIELD)
        return v?.toString()
            ?: throw IllegalStateException("写文件系统的行缺少 ${FilesystemSchemas.CONTENT_FIELD} 字段")
    }

    private fun outputPath(): String {
        val ext = when (config.fileFormat) {
            "csv" -> "csv"
            "xlsx" -> "xlsx"
            else -> "txt"
        }
        val name = config.fileName.ifBlank { "output.$ext" }
        return if (config.path.endsWith("/")) config.path + name else config.path
    }

    private fun newConf(): Configuration = Configuration().apply {
        this["fs.defaultFS"] = config.defaultFs
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(FilesystemWriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(FilesystemWriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(FilesystemWriteFn::class.java, "records_rejected")
    }
}
