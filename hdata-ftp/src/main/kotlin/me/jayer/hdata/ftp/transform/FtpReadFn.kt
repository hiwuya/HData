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
import java.io.IOException
import java.io.Serializable
import java.nio.charset.Charset

/** A remote file to be read. Its size is obtained when listing directories at graph-construction time and used to decide splitting. */
data class FtpFile(val path: String, val size: Long) : Serializable {
    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * A Splittable DoFn that reads FTP files in parallel by **byte range**.
 *
 * The restriction is the in-file byte range `[from, to)`, located at the start via FTP's `REST`
 * command (`setRestartOffset`). Row ownership follows Beam's `TextSource` convention: **the line
 * that crosses over `from` belongs to the previous shard**, so a non-zero start must first discard
 * the first incomplete line; conversely, a line whose start falls within `[from, to)` is read to
 * completion by this shard even if it crosses `to`. This way adjacent shards neither overlap nor miss.
 *
 * Compared with the pre-refactor version:
 *  - that version retrieved the whole file into a `ByteArrayOutputStream` via `retrieveFile` — a file
 *    of hundreds of MB would blow up the worker;
 *  - the restriction was fixed to `OffsetRange(0, 1)`, so a single file could only be read from start
 *    to end by one worker;
 *  - when `retrieveFile` returned false it only logged a warning and `return`ed, **the whole file was
 *    silently dropped** yet the job still succeeded.
 *
 * The `csv` format is not split by range: a quoted field can embed newlines, so cutting at an
 * arbitrary byte position would split a record in half.
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
    fun getInitialRestriction(@Element file: FtpFile): OffsetRange {
        val byteSplittable = config.fileFormat == FtpReadConfig.TEXT &&
            byteLineCompatible(Charset.forName(config.encoding))
        // csv and non-ASCII-compatible encodings must be a single logical work unit. Returning the
        // whole range from @SplitRestriction alone is not enough: the OffsetRangeTracker can still
        // dynamically split off residual ranges at runtime, and the residual task would read the
        // whole file again, causing duplication.
        return if (byteSplittable) OffsetRange(0, file.size.coerceAtLeast(0)) else OffsetRange(0, 1)
    }

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
        if (config.fileFormat != FtpReadConfig.TEXT || !byteLineCompatible(checkNotNull(charset))) {
            // csv and text encodings whose line break is not a single byte must be read as a whole
            // file; see the class comment
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
            // claim past the end of the range to tell the tracker this segment is done; without this,
            // checkDone() would report "claiming work in [x, y) was not attempted"
            tracker.tryClaim(range.to)
            return
        }
        if (!byteLineCompatible(checkNotNull(charset))) {
            if (!tracker.tryClaim(range.from)) return
            readWholeText(file, receiver)
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
        // a non-zero start must read from from-1, not from: when exactly one line starts at from,
        // then from-1 is the previous line's newline; the readLine() below only consumes that newline,
        // and this line still belongs to this shard. Reading directly from from and dropping the first
        // line would make this whole line disappear out of nowhere — the previous shard stopped when
        // its position reached from, so it won't read it either. Beam's TextSource and this project's
        // HiveTextRecordReader both do it this way.
        val start = if (range.from > 0) range.from - 1 else 0L
        openStream(file.path, start).use { stream ->
            val reader = ByteLineReader(stream, start)
            // non-zero start: the line that crosses the boundary belongs to the previous shard, drop it here first
            if (range.from > 0 && reader.readLine() == null) {
                // the range start is already past EOF. Still must claim one offset past the end,
                // otherwise checkDone() reports "claiming work in [x, y) was not attempted"
                tracker.tryClaim(range.to)
                return
            }
            while (true) {
                // we claim the line's starting offset: if the start falls within this range, this shard
                // is responsible for reading the whole line. On overflow tryClaim returns false and records
                // the attempt, so checkDone() considers this segment done
                if (!tracker.tryClaim(reader.position)) {
                    break
                }
                val bytes = reader.readLine()
                if (bytes == null) {
                    // file finished (this shard is the last segment); claim past the end to mark completion
                    tracker.tryClaim(range.to)
                    break
                }
                receiver.output(textRow(String(bytes, checkNotNull(charset))))
                count++
            }
        }
        RECORDS_READ.inc(count)
        LOGGER.info("FTP file[{}] range [{}, {}) read {} lines", file.path, range.from, range.to, count)
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
        LOGGER.info("FTP file[{}] read {} lines", file.path, count)
    }

    private fun readWholeText(file: FtpFile, receiver: OutputReceiver<Row>) {
        var count = 0L
        openStream(file.path, 0).bufferedReader(checkNotNull(charset)).useLines { lines ->
            lines.forEach {
                receiver.output(textRow(it))
                count++
            }
        }
        RECORDS_READ.inc(count)
        LOGGER.info("FTP file[{}] read whole file with encoding={} yielding {} lines", file.path, config.encoding, count)
    }

    /**
     * Open a download stream starting at [offset].
     *
     * `retrieveFileStream` returning null means the server rejected this download — in the pre-refactor
     * version the corresponding `retrieveFile` returning false only logged a warning and continued, and
     * the whole file was just gone.
     */
    private fun openStream(path: String, offset: Long): InputStream {
        val c = checkNotNull(client) { "FTP client not initialized" }
        // FTPClient retains the REST offset; even when reading from the start we must explicitly clear
        // it, otherwise reusing the client may carry over the previous offset.
        c.restartOffset = offset
        val stream = c.retrieveFileStream(path)
            ?: throw IllegalStateException("Unable to read FTP file[$path] (offset=$offset): ${c.replyString}")
        return FtpStream(c, BufferedInputStream(stream))
    }

    private fun textRow(line: String): Row =
        Row.withSchema(FTP_TEXT_SCHEMA).addValue(line).build()

    private fun csvRow(target: Schema, values: List<String?>, source: String, lineNumber: Long): Row {
        require(values.size <= target.fieldCount) {
            "$source line $lineNumber has ${values.size} columns, exceeding the ${target.fieldCount} declared in schema_fields"
        }
        val builder = Row.withSchema(target)
        target.fields.forEachIndexed { index, field ->
            val raw = values.getOrNull(index)?.takeIf { it.isNotBlank() }
            builder.addValue(coerce(raw, field, index, source, lineNumber))
        }
        return builder.build()
    }

    /**
     * Before the refactor this was `row.addValue(raw)` — regardless of the declared field type, the
     * value pushed in was always a string, so `file_format=csv` combined with any non-STRING field
     * was broken.
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
                "$source line $lineNumber column ${index + 1} [${field.name}] cannot be parsed as ${field.type.typeName}: \"$raw\"",
                e,
            )
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(FtpReadFn::class.java)
        private val RECORDS_READ = Metrics.counter(FtpReadFn::class.java, "records_read")

        /** Target number of bytes per split. */
        private const val SPLIT_BYTES = 64L * 1024 * 1024

        internal fun byteLineCompatible(charset: Charset): Boolean {
            val name = charset.name().uppercase()
            return name == "UTF-8" || name == "US-ASCII" || name.startsWith("ISO-8859-") ||
                name.startsWith("WINDOWS-") || name in setOf("GBK", "GB18030", "BIG5", "SHIFT_JIS")
        }
    }
}

/**
 * A wrapper around the download stream: supplements `completePendingCommand()` on close.
 *
 * After FTP's data connection closes, the reply on the control connection must be read once more;
 * missing this step makes the next command get a misaligned response, manifesting as hard-to-debug
 * issues like "the second file comes out empty".
 */
private class FtpStream(private val client: FTPClient, private val delegate: InputStream) : InputStream() {

    override fun read(): Int = delegate.read()

    override fun read(b: ByteArray, off: Int, len: Int): Int = delegate.read(b, off, len)

    override fun available(): Int = delegate.available()

    override fun close() {
        var failure: Throwable? = null
        try {
            delegate.close()
        } catch (e: Throwable) {
            failure = e
        }
        val completed = try {
            client.completePendingCommand()
        } catch (e: Throwable) {
            if (failure == null) failure = e else failure.addSuppressed(e)
            false
        }
        if (!completed) {
            val error = IOException("FTP data transfer did not complete successfully: ${client.replyString}")
            if (failure == null) failure = error else failure.addSuppressed(error)
        }
        failure?.let { throw it }
    }
}

/**
 * Read lines by byte and track the offset.
 *
 * Look for the `\n` byte directly instead of decoding first: only this way can we get an accurate
 * byte offset for splitting. It is safe for ASCII-compatible encodings like UTF-8 / GBK / ASCII
 * (`0x0A` never appears inside a multi-byte sequence); fixed-width encodings like UTF-16 do not apply
 * — if one is actually encountered, the file should be read whole.
 */
private class ByteLineReader(private val stream: InputStream, startOffset: Long) {

    var position: Long = startOffset
        private set

    private val buffer = java.io.ByteArrayOutputStream(256)

    /** @return the raw bytes of a line (without the newline), or null at end of stream. */
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
                // handle CRLF
                return if (bytes.isNotEmpty() && bytes.last() == '\r'.code.toByte()) bytes.copyOf(bytes.size - 1) else bytes
            }
            buffer.write(b)
        }
    }
}
