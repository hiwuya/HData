package me.jayer.hdata.ftp.transform

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.ftp.FtpConnection
import me.jayer.hdata.ftp.FtpWriteConfig
import me.jayer.hdata.ftp.newFtpClient
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.apache.commons.net.ftp.FTPClient
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.nio.charset.Charset

/**
 * 攒批把输入行写成 FTP 远程文件的一行。`@FinishBundle` 时把整批内容通过 `appendFile` 上传，
 * 写失败且开了死信时退回整批进死信流；没开死信时异常直接抛出，作业失败。
 */
class FtpWriteFn(
    private val connection: FtpConnection,
    private val filePath: String,
    private val fileFormat: String,
    private val fieldNames: List<String>,
    private val encoding: String,
    private val batchSize: Int,
    private val inputSchema: Schema,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
) : DoFn<Row, Row>() {

    @Transient
    private var client: FTPClient? = null

    private val buffered = mutableListOf<ValueInSingleWindow<Row>>()
    private val failures = mutableListOf<ValueInSingleWindow<Row>>()

    @Setup
    fun setup() {
        client = newFtpClient(connection)
    }

    @StartBundle
    fun startBundle() {
        buffered.clear()
        failures.clear()
    }

    @ProcessElement
    fun processElement(
        @Element row: Row,
        @Timestamp timestamp: org.joda.time.Instant,
        window: BoundedWindow,
        pane: PaneInfo,
    ) {
        buffered.add(ValueInSingleWindow.of(row, timestamp, window, pane))
        if (buffered.size >= batchSize) {
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
        runCatching { client?.logout() }
        runCatching { client?.disconnect() }
        client = null
    }

    private fun flush() {
        if (buffered.isEmpty()) {
            return
        }
        val c = checkNotNull(client) { "FTP 客户端未初始化" }
        val charset = Charset.forName(encoding)
        val content = buffered.joinToString("") { record ->
            lineOf(record.value) + "\n"
        }
        try {
            val ok = c.appendFile(filePath, ByteArrayInputStream(content.toByteArray(charset)))
            if (!ok) {
                throw IllegalStateException("FTP 上传文件失败: $filePath")
            }
            RECORDS_WRITTEN.inc(buffered.size.toLong())
        } catch (e: Exception) {
            if (!deadLetter) {
                throw e
            }
            LOGGER.warn("写入 FTP 失败，转入死信: {}", e.message)
            RECORDS_REJECTED.inc()
            buffered.forEach { record ->
                failures.add(
                    ValueInSingleWindow.of(
                        ErrorSchemas.failure(errorSchema, record.value, e, transformName),
                        record.timestamp,
                        record.window,
                        record.paneInfo,
                    ),
                )
            }
        }
        buffered.clear()
    }

    private fun lineOf(row: Row): String =
        if (fileFormat == "csv") {
            fieldNames.joinToString(",") { name -> (row.getValue<Any?>(name))?.toString() ?: "" }
        } else {
            row.getString("content") ?: throw IllegalStateException("写 FTP 的行缺少 content 字段")
        }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(FtpWriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(FtpWriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(FtpWriteFn::class.java, "records_rejected")
    }
}
