package me.jayer.hdata.filesystem.transform

import me.jayer.hdata.filesystem.FilesystemWriteConfig
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import org.apache.poi.xssf.streaming.SXSSFWorkbook
import java.nio.channels.Channels
import java.nio.channels.WritableByteChannel

/**
 * 把 [Row] 写成 xlsx 的 [FileIO.Sink]。
 *
 * 用 [SXSSFWorkbook] 而不是 `XSSFWorkbook`：前者只在内存里保留最近 [ROW_WINDOW] 行，
 * 其余落到临时文件。重构前的实现把**全部行**攒在一个 `MutableList<Row>` 里，到 `@Teardown`
 * 才一次性建工作簿写出——几十万行就能把 worker 撑爆。
 *
 * 而且那次写出还包在 `runCatching { ... }` 里：写失败被**完全吞掉**，
 * 作业照常成功退出，只是产出一个空的或残缺的 xlsx。
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
        val xrow = checkNotNull(sheet) { "工作簿未打开" }.createRow(rowNum++)
        schema.fields.forEachIndexed { index, field ->
            require(element.schema.hasField(field.name)) {
                "输入行缺少 schema_fields 声明的字段[${field.name}]，现有字段: ${element.schema.fieldNames}"
            }
            setCell(xrow.createCell(index), element.getValue<Any?>(field.name), field.type)
        }
    }

    override fun flush() {
        val wb = checkNotNull(workbook) { "工作簿未打开" }
        try {
            // 异常必须往外抛：写失败却让作业成功是最糟的结果
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
            Schema.TypeName.INT64,
            Schema.TypeName.BYTE,
            Schema.TypeName.FLOAT,
            Schema.TypeName.DOUBLE -> cell.setCellValue((value as Number).toDouble())

            Schema.TypeName.BOOLEAN -> cell.setCellValue(value as Boolean)
            else -> cell.setCellValue(value.toString())
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1

        /** 内存里保留的行数，其余由 POI 落到临时文件。 */
        private const val ROW_WINDOW = 1000
    }
}
