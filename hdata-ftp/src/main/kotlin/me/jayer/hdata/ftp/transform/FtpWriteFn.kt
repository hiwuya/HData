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
 * Write input rows to a **sharded** file on FTP.
 *
 * Each bundle is first written to a `.tmp` temp name; after `@FinishBundle` succeeds it is `rename`d
 * to the final shard name — if it fails midway, only a half-written `.tmp` file is left behind,
 * which downstream will not treat as a real output.
 *
 * Before the refactor every instance appended to the same `file_name` via `appendFile`:
 *  - multiple instances appending concurrently would interleave the contents;
 *  - re-running the job would append **after** the previous run's result rather than overwriting it,
 *    so two runs yield two copies of the data.
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

    /** The first upload of this bundle uses storeFile (overwrite + write header); subsequent ones use appendFile. */
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
        shard = UUID.randomUUID().toString()
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
        val buffer = checkNotNull(lines) { "writer not initialized" }
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
        val c = checkNotNull(client) { "FTP client not initialized" }
        val first = firstUpload
        val body = buildString {
            if (first && config.header && config.fileFormat == FtpReadConfig.CSV) {
                val writer = StringWriter()
                checkNotNull(csvFormat).print(writer).use { it.printRecord(config.outputFieldNames) }
                append(writer.toString().trimEnd('\r', '\n')).append('\n')
            }
            buffer.forEach { append(it).append('\n') }
        }
        val bytes = body.toByteArray(checkNotNull(charset))
        val ok = java.io.ByteArrayInputStream(bytes).use { stream ->
            // first time use storeFile: in case the temp name collides with a leftover file, overwrite
            // rather than append
            if (first) c.storeFile(tempPath(), stream) else c.appendFile(tempPath(), stream)
        }
        firstUpload = false
        // an upload failure must be thrown. Before the refactor the return value was ignored, so a
        // failed write was still treated as success
        check(ok) { "Failed to upload FTP file[${tempPath()}]: ${c.replyString}" }
        RECORDS_WRITTEN.inc(buffer.size.toLong())
        buffer.clear()
    }

    /** Rename the temp file to the final shard name; in the rare case of a name clash, delete the old target first to avoid append leftovers. */
    private fun commit() {
        val c = checkNotNull(client)
        val temp = tempPath()
        val target = config.shardPath(checkNotNull(shard))
        if (c.listFiles(temp).isEmpty()) {
            // this bundle wrote nothing, so there is nothing to commit
            return
        }
        runCatching { c.deleteFile(target) }
        check(c.rename(temp, target)) { "Failed to rename $temp to $target: ${c.replyString}" }
        LOGGER.info("FTP shard write complete: {}", target)
    }

    private fun format(row: Row): String {
        if (config.fileFormat == FtpReadConfig.CSV) {
            val values = config.outputFieldNames.map { name ->
                require(row.schema.hasField(name)) {
                    "Row written to FTP is missing field [$name] declared in schema_fields; existing fields: ${row.schema.fieldNames}"
                }
                row.getValue<Any?>(name)?.toString()
            }
            val writer = StringWriter()
            checkNotNull(csvFormat).print(writer).use { it.printRecord(values) }
            return writer.toString().trimEnd('\r', '\n')
        }
        require(row.schema.hasField("content")) {
            "file_format=text requires the input row to have a content (STRING) field; existing fields: ${row.schema.fieldNames}"
        }
        return row.getString("content")
            ?: throw IllegalArgumentException("content field is null, cannot write a text line")
    }

    private fun reject(record: ValueInSingleWindow<Row>, e: Exception) {
        if (!deadLetter) {
            throw e
        }
        LOGGER.warn("Failed to write to FTP, routing to dead letter: {}", e.message)
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
