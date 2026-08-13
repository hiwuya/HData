package me.jayer.hdata.ftp.transform

import me.jayer.hdata.ftp.FtpConnection
import me.jayer.hdata.ftp.buildReadSchema
import me.jayer.hdata.ftp.csvLineToRow
import me.jayer.hdata.ftp.newFtpClient
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.values.Row
import org.apache.commons.net.ftp.FTPClient
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.nio.charset.Charset

/**
 * 按文件切分并行读 FTP。元素是一个远程文件路径（String），限制用 `OffsetRange(0,1)` 表示"整文件"，
 * 交给 Beam 在运行时把多个文件分散到不同 worker（文件本身作为一个不可中断的读取单元）。
 *
 * `@ProcessElement` 用 `tryClaim(0)` 一次性认领整段，下载文件后逐行 Emit 一条 Row。
 */
@DoFn.BoundedPerElement
class FtpReadFn(
    private val connection: FtpConnection,
    private val fileFormat: String,
    private val schemaFields: List<String>?,
    private val encoding: String,
    private val outputSchema: Schema,
) : DoFn<String, Row>() {

    @Transient
    private var client: FTPClient? = null

    @Setup
    fun setup() {
        client = newFtpClient(connection)
    }

    @Teardown
    fun tearDown() {
        runCatching { client?.logout() }
        runCatching { client?.disconnect() }
        client = null
    }

    @GetInitialRestriction
    fun getInitialRestriction(@Element file: String): OffsetRange = OffsetRange(0, 1)

    @SplitRestriction
    fun splitRestriction(
        @Element file: String,
        @Restriction restriction: OffsetRange,
        receiver: OutputReceiver<OffsetRange>,
    ) {
        restriction.split(1, 1).forEach { receiver.output(it) }
    }

    @ProcessElement
    fun processElement(
        @Element file: String,
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
        val c = checkNotNull(client) { "FTP 客户端未初始化" }
        val bytes = ByteArrayOutputStream()
        if (!c.retrieveFile(file, bytes)) {
            LOGGER.warn("无法读取 FTP 文件: {}", file)
            return
        }
        val charset = Charset.forName(encoding)
        val content = bytes.toString(charset)
        var count = 0L
        for (line in content.lineSequence()) {
            if (line.isEmpty()) continue
            val row = if (fileFormat == "csv") csvLineToRow(outputSchema, line) else textRow(line)
            receiver.output(row)
            count++
        }
        RECORDS_READ.inc(count)
        LOGGER.info("FTP 文件[{}] 读出 {} 行", file, count)
    }

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = restriction.newTracker()

    @GetRestrictionCoder
    fun restrictionCoder(): Coder<OffsetRange> = OffsetRange.Coder()

    private fun textRow(line: String): Row =
        Row.withSchema(outputSchema).addValue(line).build()

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(FtpReadFn::class.java)
        private val RECORDS_READ = Metrics.counter(FtpReadFn::class.java, "records_read")
    }
}
