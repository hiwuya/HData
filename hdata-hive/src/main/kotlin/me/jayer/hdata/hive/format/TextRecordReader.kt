package me.jayer.hdata.hive.format

import me.jayer.hdata.hive.split.HiveFile
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory
import java.io.InputStream

/**
 * Reader for TEXTFILE: split by byte range, one record per line, field encoding through [LazySimpleCodec].
 *
 * Line ownership follows Hadoop's `LineRecordReader` convention, which is also Beam's `TextSource` convention: **the line
 * crossing `from` belongs to the previous split**, so a non-zero start must drop the first incomplete line; conversely, a line
 * starting inside `[from, to)` is read to its end by this split even when it crosses `to`. Neighbouring splits are therefore
 * neither overlapping nor leaking.
 *
 * A whole-file compressed (`.gz` / `.snappy` / `.lz4`) file can only be decompressed from the start, and such files are
 * already judged unsplittable in [me.jayer.hdata.hive.split.HiveFileSystems.isSplittable],
 * so the range obtained is necessarily the whole file.
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
            // A compressed file is not splittable, the whole file is one split
            if (!claim.tryClaim(range.from)) {
                return false
            }
            compression.createInputStream(input).use { decompressed ->
                readLines(LineReader(decompressed, 0), claimEveryLine = false, claim = claim, output = output)
            }
            return true
        }

        // A non-zero start must read from from-1, not from: if a line starts exactly at from, then from-1 holds the previous
        // line's newline, and this readLine() only swallows that newline, so the line still belongs to this split. Reading from
        // `from` and dropping the first line would lose a whole line in that case —
        // the previous split stopped once its position reached from, so it will not read it either.
        // Beam's TextSource does exactly this.
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
     * @param claimEveryLine when splittable, claim the start offset line by line; when not, claim the whole range once (caller side)
     */
    private fun readLines(
        reader: LineReader,
        claimEveryLine: Boolean,
        claim: OffsetClaim,
        output: (Row) -> Unit,
    ): Boolean {
        val footer = spec.footerLineCount
        // Dropping the trailing N lines means buffering N lines first: only when line N+1 is read can line 1 be known not to be
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
