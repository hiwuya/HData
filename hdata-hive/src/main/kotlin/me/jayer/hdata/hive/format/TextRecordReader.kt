package me.jayer.hdata.hive.format

import me.jayer.hdata.hive.split.HiveFile
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory
import java.io.InputStream

/**
 * TEXTFILE 的读取器：按字节区间切，一行一条记录，字段编码走 [LazySimpleCodec]。
 *
 * 行的归属沿用 Hadoop `LineRecordReader` 的约定，也是 Beam `TextSource` 的约定：
 * **跨过 from 的那一行属于上一个分片**，所以非 0 起点要先丢掉第一个不完整的行；
 * 反过来，起始位置落在 `[from, to)` 内的行即使跨过了 to 也由本分片读完。
 * 相邻分片因此既不重也不漏。
 *
 * 整文件压缩（`.gz` / `.snappy` / `.lz4`）时只能从头解压，
 * 这种文件在 [me.jayer.hdata.hive.split.HiveFileSystems.isSplittable] 里已经判成不可切，
 * 拿到的区间必然是整个文件。
 *
 * @author wuya
 */
class TextRecordReader(
    private val file: HiveFile,
    private val range: OffsetRange,
    spec: HiveReadSpec,
    partitionValues: List<Any?>,
    serdeParameters: Map<String, String>,
    private val configuration: Configuration,
) : HiveRecordReader(spec, partitionValues) {

    private val codec = LazySimpleCodec(serdeParameters)
    private val fieldTypes = spec.dataFieldTypes
    private var stream: InputStream? = null

    override fun read(claim: OffsetClaim, output: (Row) -> Unit): Boolean {
        val path = Path(file.path)
        val fs = path.getFileSystem(configuration)
        val compression = CompressionCodecFactory(configuration).getCodec(path)
        val input = fs.open(path)
        stream = input

        if (compression != null) {
            // 压缩文件不可切分，整个文件就是一个分片
            if (!claim.tryClaim(range.from)) {
                return false
            }
            compression.createInputStream(input).use { decompressed ->
                readLines(LineReader(decompressed, 0), claimEveryLine = false, claim = claim, output = output)
            }
            return true
        }

        // 非 0 起点要从 from-1 开始读，而不是 from：
        // 如果恰好有一行从 from 开始，那么 from-1 上就是上一行的换行符，
        // 下面这次 readLine() 只会吃掉那个换行符，这一行仍然归本分片。
        // 直接从 from 开始读再丢掉第一行的话，这种情况下会把一整行丢掉——
        // 而上一个分片在 position 到达 from 时就停了，也不会读它。Beam 的 TextSource 就是这么处理的。
        val start = if (range.from > 0) range.from - 1 else 0L
        input.seek(start)
        val reader = LineReader(input, start)
        if (range.from > 0 && reader.readLine() == null) {
            return true
        }
        if (range.from == 0L) {
            repeat(spec.headerLineCount) { reader.readLine() }
        }
        return readLines(reader, claimEveryLine = true, claim = claim, output = output)
    }

    /**
     * @param claimEveryLine 可切分时逐行认领起始偏移量；不可切分时整段只认领一次（在调用方做）
     */
    private fun readLines(
        reader: LineReader,
        claimEveryLine: Boolean,
        claim: OffsetClaim,
        output: (Row) -> Unit,
    ): Boolean {
        val footer = spec.footerLineCount
        // 要丢掉末尾 N 行就得先把 N 行攒住：读到第 N+1 行时才能确定第 1 行不是末尾行
        val pending = if (footer > 0) ArrayDeque<ByteArray>(footer + 1) else null
        while (true) {
            if (claimEveryLine && !claim.tryClaim(reader.position)) {
                return false
            }
            val bytes = reader.readLine() ?: break
            if (pending == null) {
                output(toRow(decode(bytes)))
                continue
            }
            pending.addLast(bytes)
            if (pending.size > footer) {
                output(toRow(decode(pending.removeFirst())))
            }
        }
        return true
    }

    private fun decode(bytes: ByteArray): Array<Any?> =
        codec.decodeRow(String(bytes, codec.charset), fieldTypes, spec.projectedDataIndexes)

    override fun close() {
        stream?.close()
        stream = null
    }
}
