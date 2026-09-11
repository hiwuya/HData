package me.jayer.hdata.hive.format

import me.jayer.hdata.hive.split.HiveFile
import me.jayer.hdata.hive.type.HiveValues
import org.apache.beam.sdk.values.Row
import org.apache.commons.csv.CSVFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory
import java.io.InputStream
import java.io.InputStreamReader

/**
 * Reader for `OpenCSVSerde`.
 *
 * **Reads the whole file, no range splitting**: a quoted CSV field may contain newlines, so cutting at an arbitrary byte
 * position would split a record in half. Parallelism can only come from the number of files, the same reasoning as the CSV
 *
 * Hive's `OpenCSVSerde` reads every column as a string; this is a little more lenient: values are parsed according to the types
 * declared on the table, and anything that fails to parse becomes null (consistent with [HiveValues.parseString]). If the table
 * declares everything as string the way Hive does, the behaviour is exactly the same as Hive.
 *
 * @author wuya
 */
class CsvRecordReader(
    private val file: HiveFile,
    spec: HiveReadSpec,
    partitionValues: List<Any?>,
    private val serdeParameters: Map<String, String>,
    private val configuration: Configuration,
) : HiveRecordReader(spec, partitionValues) {

    private val fieldTypes = spec.dataFieldTypes
    private var stream: InputStream? = null

    override fun read(claim: OffsetClaim, output: (Row) -> Unit): Boolean {
        if (!claim.tryClaim(0L)) {
            return false
        }
        val path = Path(file.path)
        val fs = path.getFileSystem(configuration)
        val compression = CompressionCodecFactory(configuration).getCodec(path)
        val raw = fs.open(path)
        stream = raw
        val input = compression?.createInputStream(raw) ?: raw

        InputStreamReader(input).use { reader ->
            val iterator = csvFormat().parse(reader).iterator()
            repeat(spec.headerLineCount) { if (iterator.hasNext()) iterator.next() }
            val footer = spec.footerLineCount
            val pending = if (footer > 0) ArrayDeque<List<String>>(footer + 1) else null
            while (iterator.hasNext()) {
                val record = iterator.next().toList()
                if (pending == null) {
                    output(toRow(decode(record)))
                    continue
                }
                pending.addLast(record)
                if (pending.size > footer) {
                    output(toRow(decode(pending.removeFirst())))
                }
            }
        }
        return true
    }

    /** The three `OpenCSVSerde` parameters, with the same defaults as Hive. */
    private fun csvFormat(): CSVFormat = CSVFormat.DEFAULT.builder()
        .setDelimiter(charParam("separatorChar", ','))
        .setQuote(charParam("quoteChar", '"'))
        .setEscape(charParam("escapeChar", '\\'))
        .setIgnoreEmptyLines(true)
        .build()

    private fun charParam(name: String, fallback: Char): Char =
        serdeParameters[name]?.takeIf { it.isNotEmpty() }?.first() ?: fallback

    private fun decode(record: List<String>): Array<Any?> = Array(spec.projectedDataIndexes.size) { i ->
        val index = spec.projectedDataIndexes[i]
        record.getOrNull(index)?.let { text ->
            if (text == HiveValues.DEFAULT_NULL_FORMAT) null else HiveValues.parseString(text, fieldTypes[index])
        }
    }

    override fun close() {
        stream?.close()
        stream = null
    }
}
