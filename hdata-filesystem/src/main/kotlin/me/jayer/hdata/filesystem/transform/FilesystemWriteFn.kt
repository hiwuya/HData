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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory
import java.net.URI
import java.nio.charset.Charset

/**
 * 逐行（攒批）写入文件系统。写失败且开了死信时退回逐条写，真正写不进去的记录进死信流；
 * 没开死信时异常直接抛出，作业失败。
 */
class FilesystemWriteFn(
    private val config: FilesystemWriteConfig,
    private val inputSchema: Schema,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
) : DoFn<Row, Row>() {

    @Transient
    private var fs: FileSystem? = null

    @Transient
    private var writer: java.io.BufferedWriter? = null

    private val buffered = mutableListOf<ValueInSingleWindow<Row>>()
    private val failures = mutableListOf<ValueInSingleWindow<Row>>()

    @Setup
    fun setup() {
        val fileSystem = FileSystem.get(URI(config.defaultFs), newConf())
        fs = fileSystem
        val out = fileSystem.create(Path(outputPath()), true)
        writer = out.bufferedWriter(Charset.forName(config.encoding))
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
        if (buffered.size >= config.batchSize) {
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
        runCatching { writer?.flush() }
        runCatching { writer?.close() }
        writer = null
        runCatching { fs?.close() }
        fs = null
    }

    private fun flush() {
        if (buffered.isEmpty()) {
            return
        }
        val w = checkNotNull(writer) { "写入流未初始化" }
        buffered.forEach { record ->
            try {
                w.write(formatRow(record.value))
                w.newLine()
                RECORDS_WRITTEN.inc()
            } catch (e: Exception) {
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
        }
        runCatching { w.flush() }
        buffered.clear()
    }

    private fun formatRow(row: Row): String = when (config.fileFormat) {
        "csv" -> config.schemaFields.mapIndexed { index, _ ->
            val v: Any? = row.getValue(index)
            v?.toString() ?: ""
        }.joinToString(",")
        else -> row.getString(FilesystemSchemas.CONTENT_FIELD)
            ?: throw IllegalStateException("写文件系统的行缺少 ${FilesystemSchemas.CONTENT_FIELD} 字段")
    }

    private fun outputPath(): String {
        val name = config.fileName.ifBlank {
            "output.${if (config.fileFormat == "csv") "csv" else "txt"}"
        }
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
