package me.jayer.hdata.filesystem.transform

import me.jayer.hdata.filesystem.FilesystemWriteConfig
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import org.apache.poi.xssf.streaming.SXSSFWorkbook
import java.nio.channels.Channels
import java.nio.channels.WritableByteChannel

/**
 * A [FileIO.Sink] that writes [Row] as xlsx.
 *
 * [SXSSFWorkbook] is used instead of `XSSFWorkbook` because the former only keeps the most recent
 * [ROW_WINDOW] rows in memory and spills the rest to a temp file. The pre-refactor implementation
 * accumulated **every row** in a `MutableList<Row>` and only built the workbook once in `@Teardown`
 * -- a few hundred thousand rows were enough to blow up the worker.
 *
 * Worse, that write was wrapped in `runCatching { ... }`: a write failure was **swallowed whole**,
 * the job still exited successfully, and all you got was an empty or truncated xlsx.
 *
 * @author wuya
 */
class XlsxSink(
    private val config: FilesystemWriteConfig,
    private val schema: Schema,
) : FileIO.Sink<Row> {

    @Transient
    private var workbook: SXSSFWorkbook? = null

    @Transient
    private var sheet: org.apache.poi.xssf.streaming.SXSSFSheet? = null

    @Transient
    private var channel: WritableByteChannel? = null

    private var rowNum = 0

    override fun open(channel: WritableByteChannel) {
        this.channel = channel
        val wb = SXSSFWorkbook(ROW_WINDOW)
        workbook = wb
        sheet = wb.createSheet(config.sheet.ifBlank { "Sheet1" })
        rowNum = 0
        if (config.header) {
            val header = checkNotNull(sheet).createRow(rowNum++)
            schema.fields.forEachIndexed { index, field -> header.createCell(index).setCellValue(field.name) }
        }
    }

    override fun write(element: Row) {
        val xrow = checkNotNull(sheet) { "workbook is not open" }.createRow(rowNum++)
        schema.fields.forEachIndexed { index, field ->
            require(element.schema.hasField(field.name)) {
                "input row is missing field [${field.name}] declared in schema_fields; existing fields: ${element.schema.fieldNames}"
            }
            setCell(xrow.createCell(index), element.getValue<Any?>(field.name), field.type)
        }
    }

    override fun flush() {
        val wb = checkNotNull(workbook) { "workbook is not open" }
        try {
            // the exception must propagate: a failed write that still lets the job succeed is the worst outcome
            val output = Channels.newOutputStream(checkNotNull(channel))
            wb.write(output)
            output.flush()
        } finally {
            runCatching { wb.dispose() }
            runCatching { wb.close() }
            workbook = null
            sheet = null
        }
    }

    private fun setCell(cell: org.apache.poi.ss.usermodel.Cell, value: Any?, type: Schema.FieldType) {
        if (value == null) {
            cell.setBlank()
            return
        }
        when (type.typeName) {
            Schema.TypeName.INT16,
            Schema.TypeName.INT32,
            Schema.TypeName.BYTE -> cell.setCellValue((value as Number).toDouble())

            Schema.TypeName.INT64 -> {
                val long = (value as Number).toLong()
                if (long in MIN_EXACT_DOUBLE_INTEGER..MAX_EXACT_DOUBLE_INTEGER) {
                    cell.setCellValue(long.toDouble())
                } else {
                    // xlsx numeric cells are IEEE-754 Doubles underneath. Storing a Long beyond 2^53
                    // silently changes the value; written as text, the read side still parses it back
                    // to INT64 exactly via schema_fields.
                    cell.setCellValue(long.toString())
                }
            }

            Schema.TypeName.FLOAT,
            Schema.TypeName.DOUBLE -> cell.setCellValue((value as Number).toDouble())

            Schema.TypeName.BOOLEAN -> cell.setCellValue(value as Boolean)
            else -> cell.setCellValue(value.toString())
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1

        /** Rows kept in memory; POI spills the rest to a temp file. */
        private const val ROW_WINDOW = 1000
        private const val MAX_EXACT_DOUBLE_INTEGER = 9_007_199_254_740_991L
        private const val MIN_EXACT_DOUBLE_INTEGER = -MAX_EXACT_DOUBLE_INTEGER
    }
}
