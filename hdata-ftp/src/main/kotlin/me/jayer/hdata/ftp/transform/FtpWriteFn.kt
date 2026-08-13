package me.jayer.hdata.ftp.transform

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.ftp.FtpReadConfig
import me.jayer.hdata.ftp.FtpWriteConfig
import me.jayer.hdata.ftp.newFtpClient
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.apache.commons.csv.CSVFormat
import org.apache.commons.net.ftp.FTPClient
import org.slf4j.LoggerFactory
import java.io.StringWriter
import java.nio.charset.Charset
import java.util.UUID

/**
 * 把输入行写成 FTP 上的一个**分片**文件。
 *
 * 每个 bundle 先写到一个 `.tmp` 临时名上，`@FinishBundle` 成功后再 `rename` 到最终分片名——
 * 中途失败留下的只是一个带 `.tmp` 后缀的半成品，不会被下游当成正式产出。
 *
 * 重构前所有实例都往同一个 `file_name` 上 `appendFile`：
 *  - 多个实例并发追加，内容会交错在一起；
 *  - 重跑作业是往上一次的结果**后面接着追加**，不是覆盖，跑两遍就有两份数据。
 *
 * @author wuya
 */
class FtpWriteFn(
    private val config: FtpWriteConfig,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
) : DoFn<Row, Row>() {

    @Transient
    private var client: FTPClient? = null

    @Transient
    private var charset: Charset? = null

    @Transient
    private var csvFormat: CSVFormat? = null

    @Transient
    private var lines: MutableList<String>? = null

    @Transient
    private var failures: MutableList<ValueInSingleWindow<Row>>? = null

    @Transient
    private var shard: String? = null

    /** 本 bundle 的第一次上传用 storeFile（覆盖 + 写表头），之后才是 appendFile。 */
    @Transient
    private var firstUpload = true

    @Setup
    fun setup() {
        client = newFtpClient(config.connection)
        charset = Charset.forName(config.encoding)
        csvFormat = CSVFormat.DEFAULT.builder()
            .setDelimiter(config.csvDelimiter[0])
            .setQuote(config.csvQuote[0])
            .build()
    }

    @Teardown
    fun tearDown() {
        runCatching { client?.logout() }
        runCatching { client?.disconnect() }
        client = null
    }

    @StartBundle
    fun startBundle() {
        lines = mutableListOf()
        failures = mutableListOf()
        shard = UUID.randomUUID().toString().substring(0, 8)
        firstUpload = true
    }

    @ProcessElement
    fun processElement(
        @Element row: Row,
        @Timestamp timestamp: org.joda.time.Instant,
        window: BoundedWindow,
        pane: PaneInfo,
    ) {
        val record = ValueInSingleWindow.of(row, timestamp, window, pane)
        val line = try {
            format(row)
        } catch (e: Exception) {
            reject(record, e)
            return
        }
        val buffer = checkNotNull(lines) { "写入器未初始化" }
        buffer.add(line)
        if (buffer.size >= config.batchSize) {
            upload()
        }
    }

    @FinishBundle
    fun finishBundle(context: FinishBundleContext) {
        upload()
        commit()
        val rejected = checkNotNull(failures)
        rejected.forEach { context.output(it.value, it.timestamp, it.window) }
        rejected.clear()
    }

    private fun tempPath(): String = config.shardPath(checkNotNull(shard)) + ".tmp"

    private fun upload() {
        val buffer = checkNotNull(lines)
        if (buffer.isEmpty()) {
            return
        }
        val c = checkNotNull(client) { "FTP 客户端未初始化" }
        val first = firstUpload
        val body = buildString {
            if (first && config.header && config.fileFormat == FtpReadConfig.CSV) {
                append(config.outputFieldNames.joinToString(config.csvDelimiter)).append('\n')
            }
            buffer.forEach { append(it).append('\n') }
        }
        val bytes = body.toByteArray(checkNotNull(charset))
        val ok = java.io.ByteArrayInputStream(bytes).use { stream ->
            // 第一次用 storeFile：万一临时名撞上了残留文件，也是覆盖而不是接在后面
            if (first) c.storeFile(tempPath(), stream) else c.appendFile(tempPath(), stream)
        }
        firstUpload = false
        // 上传失败必须抛出。重构前这里的返回值没人看，写不进去也当成功
        check(ok) { "上传 FTP 文件[${tempPath()}] 失败: ${c.replyString}" }
        RECORDS_WRITTEN.inc(buffer.size.toLong())
        buffer.clear()
    }

    /** 把临时文件改名成最终分片名；同名的旧文件先删掉，保证重跑是覆盖而不是追加。 */
    private fun commit() {
        val c = checkNotNull(client)
        val temp = tempPath()
        val target = config.shardPath(checkNotNull(shard))
        if (c.listFiles(temp).isEmpty()) {
            // 这个 bundle 一条都没写出去，没有要提交的东西
            return
        }
        runCatching { c.deleteFile(target) }
        check(c.rename(temp, target)) { "把 $temp 改名为 $target 失败: ${c.replyString}" }
        LOGGER.info("FTP 分片写入完成: {}", target)
    }

    private fun format(row: Row): String {
        if (config.fileFormat == FtpReadConfig.CSV) {
            val values = config.outputFieldNames.map { name ->
                require(row.schema.hasField(name)) {
                    "写 FTP 的行缺少 schema_fields 声明的字段[$name]，现有字段: ${row.schema.fieldNames}"
                }
                row.getValue<Any?>(name)?.toString()
            }
            val writer = StringWriter()
            checkNotNull(csvFormat).print(writer).use { it.printRecord(values) }
            return writer.toString().trimEnd('\r', '\n')
        }
        require(row.schema.hasField("content")) {
            "file_format=text 要求输入行有 content(STRING) 字段，现有字段: ${row.schema.fieldNames}"
        }
        return row.getString("content")
            ?: throw IllegalArgumentException("content 字段是 null，写不出一行文本")
    }

    private fun reject(record: ValueInSingleWindow<Row>, e: Exception) {
        if (!deadLetter) {
            throw e
        }
        LOGGER.warn("写入 FTP 失败，转入死信: {}", e.message)
        RECORDS_REJECTED.inc()
        checkNotNull(failures).add(
            ValueInSingleWindow.of(
                ErrorSchemas.failure(errorSchema, record.value, e, transformName),
                record.timestamp,
                record.window,
                record.paneInfo,
            )
        )
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(FtpWriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(FtpWriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(FtpWriteFn::class.java, "records_rejected")
    }
}
