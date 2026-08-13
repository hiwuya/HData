package me.jayer.hdata.filesystem.transform

import me.jayer.hdata.filesystem.FilesystemReadConfig
import me.jayer.hdata.filesystem.FilesystemSchemas
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory
import java.net.URI
import java.nio.charset.Charset

/**
 * 按文件切分并行读文件系统：元素是一个文件路径（String），限制用 `OffsetRange(0,1)` 表示"整个文件一段"，
 * 交给 Beam 的 splittable DoFn 框架处理（每个文件一个 element 一次处理）。
 *
 * `@ProcessElement` 打开 `FSDataInputStream`，逐行读出，每行产出一条 Row。
 */
@DoFn.BoundedPerElement
class FilesystemReadFn(
    private val config: FilesystemReadConfig,
    private val schema: Schema,
) : DoFn<String, Row>() {

    @Transient
    private var fs: FileSystem? = null

    @Setup
    fun setup() {
        fs = FileSystem.get(URI(config.defaultFs), newConf())
    }

    @Teardown
    fun tearDown() {
        runCatching { fs?.close() }
        fs = null
    }

    @GetInitialRestriction
    fun getInitialRestriction(@Element path: String): OffsetRange = OffsetRange(0, 1)

    @SplitRestriction
    fun splitRestriction(
        @Element path: String,
        @Restriction restriction: OffsetRange,
        receiver: OutputReceiver<OffsetRange>,
    ) {
        receiver.output(restriction)
    }

    @ProcessElement
    fun processElement(
        @Element path: String,
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
        val fileSystem = checkNotNull(fs) { "FileSystem 未初始化" }
        val charset = Charset.forName(config.encoding)
        var count = 0L
        fileSystem.open(Path(path)).use { inStream ->
            inStream.bufferedReader(charset).use { reader ->
                var line: String?
                while (reader.readLine().also { line = it } != null) {
                    val l = line ?: continue
                    receiver.output(toRow(l))
                    count++
                }
            }
        }
        RECORDS_READ.inc(count)
        LOGGER.info("文件[{}] 读完 {} 行", path, count)
    }

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = restriction.newTracker()

    @GetRestrictionCoder
    fun restrictionCoder(): Coder<OffsetRange> = OffsetRange.Coder()

    private fun toRow(line: String): Row = when (config.fileFormat) {
        "csv" -> FilesystemSchemas.parseCsvLine(line, schema)
        else -> Row.withSchema(schema).addValue(line).build()
    }

    private fun newConf(): Configuration = Configuration().apply {
        this["fs.defaultFS"] = config.defaultFs
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(FilesystemReadFn::class.java)
        private val RECORDS_READ = Metrics.counter(FilesystemReadFn::class.java, "records_read")
    }
}
