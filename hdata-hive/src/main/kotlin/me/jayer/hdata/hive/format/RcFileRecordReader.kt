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
 * RCFile reader; the container format goes through our own [RcFileReader].
 *
 * How a cell is interpreted depends on the SerDe, of which there are two kinds:
 *  - `ColumnarSerDe` (RCTEXT): every cell is the text of that column, reusing [LazySimpleCodec];
 *  - `LazyBinaryColumnarSerDe` (RCBINARY): every cell is LazyBinary binary, going through [LazyBinaryCodec].
 *
 * The claimable boundary is the **record (row group)**: RCFile inserts a sync marker every 2000 bytes, a non-zero start first
 * locates the first sync point inside the range, and the row group crossing the start belongs to the previous split.
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
            // End of the range: this split reads only until the next sync block start crosses the split boundary; whatever
            // crosses is left to the next split, otherwise every split reads all the way to EOF (data duplicated N times).
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
            // Each column advances one cell at a time; the position is decided by the run-length encoding inside the column, so
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
