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
 * `OpenCSVSerde` 的读取器。
 *
 * **整文件读，不做区间切分**：CSV 的引号字段可以内嵌换行，从任意字节位置切开会把一条记录劈成两半。
 * 并行度只能来自文件个数，这一点和 `hdata-filesystem` / `hdata-ftp` 的 CSV 是同一个道理。
 *
 * Hive 的 `OpenCSVSerde` 把所有列都当字符串读，这里更宽松一点：按表上声明的类型解析，
 * 解析不了的落 null（与 [HiveValues.parseString] 一致）。表如果照 Hive 的规矩全声明成 string，
 * 行为就和 Hive 完全一样。
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

    /** `OpenCSVSerde` 的三个参数，默认值与 Hive 一致。 */
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
