package me.jayer.hdata.ftp.transform

import me.jayer.hdata.ftp.FTP_TEXT_SCHEMA
import me.jayer.hdata.ftp.FtpConnection
import me.jayer.hdata.ftp.FtpReadConfig
import me.jayer.hdata.ftp.buildReadSchema
import me.jayer.hdata.ftp.newFtpClient
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.values.Row
import org.apache.commons.csv.CSVFormat
import org.apache.commons.net.ftp.FTPClient
import org.slf4j.LoggerFactory
import java.io.BufferedInputStream
import java.io.InputStream
import java.io.Serializable
import java.nio.charset.Charset

/** 一个待读的远程文件。大小在构图阶段列目录时拿到，用来决定切分。 */
data class FtpFile(val path: String, val size: Long) : Serializable {
    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * 按**字节区间**并行读 FTP 文件的 Splittable DoFn。
 *
 * 限制是文件内的字节区间 `[from, to)`，靠 FTP 的 `REST` 命令（`setRestartOffset`）定位到起点。
 * 行的归属沿用 Beam `TextSource` 的约定：**跨过 from 的那一行属于上一个分片**，
 * 因此非 0 起点要先丢掉第一个不完整的行；反过来，起点落在 `[from, to)` 内的行即使跨过了 to
 * 也由本分片读完。这样相邻分片既不重也不漏。
 *
 * 相比重构前：
 *  - 那版把整个文件 `retrieveFile` 进一个 `ByteArrayOutputStream`——几百兆的文件直接把 worker 撑爆；
 *  - 限制固定 `OffsetRange(0, 1)`，一个文件只能由一个 worker 从头读到尾；
 *  - `retrieveFile` 返回 false 时只打一条 warn 就 `return`，**整个文件被静默丢掉**，作业照常成功。
 *
 * `csv` 格式不做区间切分：带引号的字段可以内嵌换行，从任意字节位置切开会把一条记录劈成两半。
 *
 * @author wuya
 */
@DoFn.BoundedPerElement
class FtpReadFn(
    private val connection: FtpConnection,
    private val config: FtpReadConfig,
) : DoFn<FtpFile, Row>() {

    @Transient
    private var client: FTPClient? = null

    @Transient
    private var schema: Schema? = null

    @Transient
    private var charset: Charset? = null

    @Setup
    fun setup() {
        client = newFtpClient(connection)
        schema = buildReadSchema(config)
        charset = Charset.forName(config.encoding)
    }

    @Teardown
    fun tearDown() {
        runCatching { client?.logout() }
        runCatching { client?.disconnect() }
        client = null
    }

    @GetInitialRestriction
    fun getInitialRestriction(@Element file: FtpFile): OffsetRange = OffsetRange(0, file.size.coerceAtLeast(0))

    @SplitRestriction
    fun splitRestriction(
        @Element file: FtpFile,
        @Restriction restriction: OffsetRange,
        receiver: OutputReceiver<OffsetRange>,
    ) {
        val span = restriction.to - restriction.from
        if (span <= 0) {
            return
        }
        if (config.fileFormat != FtpReadConfig.TEXT) {
            // csv 必须整文件读，见类注释
            receiver.output(restriction)
            return
        }
        restriction.split(SPLIT_BYTES, SPLIT_BYTES / 2).forEach { receiver.output(it) }
    }

    @ProcessElement
    fun processElement(
        @Element file: FtpFile,
        tracker: RestrictionTracker<OffsetRange, Long>,
        receiver: OutputReceiver<Row>,
    ) {
        val range = tracker.currentRestriction()
        if (range.to <= range.from) {
            return
        }
        if (config.fileFormat == FtpReadConfig.CSV) {
            if (!tracker.tryClaim(range.from)) {
                return
            }
            readCsv(file, receiver)
            // 认领到区间之外，告诉 tracker 这段已经做完；漏了这一步 checkDone() 会报
            // "claiming work in [x, y) was not attempted"
            tracker.tryClaim(range.to)
            return
        }
        readTextRange(file, range, tracker, receiver)
    }

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = restriction.newTracker()

    @GetRestrictionCoder
    fun restrictionCoder(): Coder<OffsetRange> = OffsetRange.Coder()

    private fun readTextRange(
        file: FtpFile,
        range: OffsetRange,
        tracker: RestrictionTracker<OffsetRange, Long>,
        receiver: OutputReceiver<Row>,
    ) {
        var count = 0L
        // 非 0 起点要从 from-1 开始读，而不是 from：
        // 恰好有一行从 from 开始时，from-1 上就是上一行的换行符，下面这次 readLine()
        // 只会吃掉那个换行符，这一行仍然归本分片。直接从 from 读再丢掉第一行的话，
        // 这一整行会**凭空消失**——上一个分片在 position 到达 from 时就停了，也不会读它。
        // Beam 的 TextSource 与本项目的 HiveTextRecordReader 都是这么处理的。
        val start = if (range.from > 0) range.from - 1 else 0L
        openStream(file.path, start).use { stream ->
            val reader = ByteLineReader(stream, start)
            // 非 0 起点：跨过边界的那一行归上一个分片，这里先丢掉
            if (range.from > 0 && reader.readLine() == null) {
                // 区间起点已经越过文件末尾。仍要认领一次区间外的偏移量，
                // 否则 checkDone() 会报 "claiming work in [x, y) was not attempted"
                tracker.tryClaim(range.to)
                return
            }
            while (true) {
                // 认领的是"行的起始偏移量"：起点落在本区间内就由本分片负责读完整行。
                // 越界时 tryClaim 返回 false 并记下这次尝试，checkDone() 才认这段做完了
                if (!tracker.tryClaim(reader.position)) {
                    break
                }
                val bytes = reader.readLine()
                if (bytes == null) {
                    // 文件读完了（本分片是最后一段），认领到区间之外标记完成
                    tracker.tryClaim(range.to)
                    break
                }
                receiver.output(textRow(String(bytes, checkNotNull(charset))))
                count++
            }
        }
        RECORDS_READ.inc(count)
        LOGGER.info("FTP 文件[{}] 区间 [{}, {}) 读出 {} 行", file.path, range.from, range.to, count)
    }

    private fun readCsv(file: FtpFile, receiver: OutputReceiver<Row>) {
        val format = CSVFormat.DEFAULT.builder()
            .setDelimiter(config.csvDelimiter[0])
            .setQuote(config.csvQuote[0])
            .setIgnoreEmptyLines(true)
            .build()
        val target = checkNotNull(schema)
        var count = 0L
        openStream(file.path, 0).bufferedReader(checkNotNull(charset)).use { reader ->
            val iterator = format.parse(reader).iterator()
            if (config.header && iterator.hasNext()) {
                iterator.next()
            }
            for (record in iterator) {
                receiver.output(csvRow(target, record.toList(), file.path, record.recordNumber))
                count++
            }
        }
        RECORDS_READ.inc(count)
        LOGGER.info("FTP 文件[{}] 读出 {} 行", file.path, count)
    }

    /**
     * 打开一个从 [offset] 开始的下载流。
     *
     * `retrieveFileStream` 返回 null 表示服务端拒绝了这次下载——重构前对应的
     * `retrieveFile` 返回 false 只打了条 warn 就继续，整个文件就这么没了。
     */
    private fun openStream(path: String, offset: Long): InputStream {
        val c = checkNotNull(client) { "FTP 客户端未初始化" }
        if (offset > 0) {
            c.restartOffset = offset
        }
        val stream = c.retrieveFileStream(path)
            ?: throw IllegalStateException("无法读取 FTP 文件[$path]（offset=$offset）: ${c.replyString}")
        return FtpStream(c, BufferedInputStream(stream))
    }

    private fun textRow(line: String): Row =
        Row.withSchema(FTP_TEXT_SCHEMA).addValue(line).build()

    private fun csvRow(target: Schema, values: List<String?>, source: String, lineNumber: Long): Row {
        val builder = Row.withSchema(target)
        target.fields.forEachIndexed { index, field ->
            val raw = values.getOrNull(index)?.takeIf { it.isNotBlank() }
            builder.addValue(coerce(raw, field, index, source, lineNumber))
        }
        return builder.build()
    }

    /**
     * 重构前这里是 `row.addValue(raw)`——不管字段声明成什么类型，塞进去的都是字符串，
     * 所以 `file_format=csv` 配上任何非 STRING 字段都是坏的。
     */
    private fun coerce(raw: String?, field: Schema.Field, index: Int, source: String, lineNumber: Long): Any? {
        if (raw == null) {
            return null
        }
        return try {
            when (field.type.typeName) {
                Schema.TypeName.STRING -> raw
                Schema.TypeName.INT32 -> raw.trim().toInt()
                Schema.TypeName.INT64 -> raw.trim().toLong()
                Schema.TypeName.FLOAT -> raw.trim().toFloat()
                Schema.TypeName.DOUBLE -> raw.trim().toDouble()
                Schema.TypeName.BOOLEAN -> raw.trim().toBooleanStrict()
                else -> raw
            }
        } catch (e: Exception) {
            throw IllegalArgumentException(
                "$source 第 $lineNumber 行第 ${index + 1} 列[${field.name}] 无法解析成 ${field.type.typeName}: \"$raw\"",
                e,
            )
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(FtpReadFn::class.java)
        private val RECORDS_READ = Metrics.counter(FtpReadFn::class.java, "records_read")

        /** 每个切分的目标字节数。 */
        private const val SPLIT_BYTES = 64L * 1024 * 1024
    }
}

/**
 * 下载流的包装：关闭时补上 `completePendingCommand()`。
 *
 * FTP 的数据连接关掉之后还要读一次控制连接上的应答，漏了这一步下一条命令就会拿到错位的响应，
 * 表现是"第二个文件读出来是空的"这类难查的问题。
 */
private class FtpStream(private val client: FTPClient, private val delegate: InputStream) : InputStream() {

    override fun read(): Int = delegate.read()

    override fun read(b: ByteArray, off: Int, len: Int): Int = delegate.read(b, off, len)

    override fun available(): Int = delegate.available()

    override fun close() {
        delegate.close()
        client.completePendingCommand()
    }
}

/**
 * 按字节读行并跟踪偏移量。
 *
 * 直接找 `\n` 字节而不是先解码：这样才能拿到准确的字节偏移量用于切分。
 * 对 UTF-8 / GBK / ASCII 这类兼容 ASCII 的编码是安全的（`0x0A` 不会出现在多字节序列内部），
 * UTF-16 这种定长宽字符编码不适用——真遇到了应该整文件读。
 */
private class ByteLineReader(private val stream: InputStream, startOffset: Long) {

    var position: Long = startOffset
        private set

    private val buffer = java.io.ByteArrayOutputStream(256)

    /** @return 一行的原始字节（不含换行符），流结束返回 null。 */
    fun readLine(): ByteArray? {
        buffer.reset()
        var read = 0
        while (true) {
            val b = stream.read()
            if (b < 0) {
                position += read
                return if (read == 0) null else buffer.toByteArray()
            }
            read++
            if (b == '\n'.code) {
                position += read
                val bytes = buffer.toByteArray()
                // 兼容 CRLF
                return if (bytes.isNotEmpty() && bytes.last() == '\r'.code.toByte()) bytes.copyOf(bytes.size - 1) else bytes
            }
            buffer.write(b)
        }
    }
}
