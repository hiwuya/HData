package me.jayer.hdata.hive.format

import me.jayer.hdata.hive.format.rcfile.LazyBinaryCodec
import me.jayer.hdata.hive.format.rcfile.RcFileColumn
import me.jayer.hdata.hive.format.rcfile.RcFileReader
import me.jayer.hdata.hive.split.HiveFile
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * RCFile 读取器，容器格式走自己实现的 [RcFileReader]。
 *
 * 单元格的解释按 SerDe 分两种：
 *  - `ColumnarSerDe`（RCTEXT）：每格是这一列的文本，复用 [LazySimpleCodec]；
 *  - `LazyBinaryColumnarSerDe`（RCBINARY）：每格是 LazyBinary 二进制，走 [LazyBinaryCodec]。
 *
 * 可认领的边界是**记录（行组）**：RCFile 每隔 2000 字节插一个同步标记，
 * 非 0 起点先定位到区间内的第一个同步点，跨过起点的那个行组归上一个分片。
 *
 * @author wuya
 */
class RcFileRecordReader(
    private val file: HiveFile,
    private val range: OffsetRange,
    spec: HiveReadSpec,
    partitionValues: List<Any?>,
    private val format: HiveStorageFormat,
    serdeParameters: Map<String, String>,
    private val configuration: Configuration,
) : HiveRecordReader(spec, partitionValues) {

    private val textCodec = LazySimpleCodec(serdeParameters)
    private val fieldTypes = spec.dataFieldTypes
    private var reader: RcFileReader? = null

    override fun read(claim: OffsetClaim, output: (Row) -> Unit): Boolean {
        val path = Path(file.path)
        val fs = path.getFileSystem(configuration)
        val rcReader = RcFileReader(fs.open(path), file.length, configuration)
        reader = rcReader

        if (range.from > 0) {
            rcReader.syncTo(range.from)
        }
        var claimed = -1L
        while (true) {
            val position = rcReader.position.coerceAtLeast(range.from)
            if (rcReader.position >= file.length) {
                return true
            }
            // 区间末尾：本分片只读到下一个同步块起点越过分片边界为止，
            // 越过的部分留给下一个分片，否则每个分片都会一路读到 EOF（多分片时数据重复 N 倍）。
            if (rcReader.position >= range.to) {
                return true
            }
            if (position > claimed) {
                if (!claim.tryClaim(position)) {
                    return false
                }
                claimed = position
            }
            val block = rcReader.nextBlock() ?: return true
            emit(block.rowCount, block.columns, output)
        }
    }

    private fun emit(rowCount: Int, columns: Array<RcFileColumn>, output: (Row) -> Unit) {
        repeat(rowCount) {
            // 每一列各自往前走一格，位置由列内的游程编码决定，不能跳着读
            val cells = columns.map { column -> if (column.allNull) null else column.nextCell() }
            val values = Array<Any?>(spec.projectedDataIndexes.size) { i ->
                val index = spec.projectedDataIndexes[i]
                val cell = cells.getOrNull(index) ?: return@Array null
                decode(columns[index].data, cell, index)
            }
            output(toRow(values))
        }
    }

    private fun decode(data: ByteArray, cell: IntRange, columnIndex: Int): Any? {
        val fieldType = fieldTypes[columnIndex]
        if (format == HiveStorageFormat.RCBINARY) {
            return LazyBinaryCodec.decode(data, cell, fieldType)
        }
        if (cell.isEmpty()) {
            return null
        }
        val text = String(data, cell.first, cell.last - cell.first + 1, textCodec.charset)
        return textCodec.decodeField(text, fieldType, level = 1)
    }

    override fun close() {
        reader?.close()
        reader = null
    }
}
