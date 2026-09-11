package me.jayer.hdata.hive.format.rcfile

import org.apache.hadoop.io.WritableUtils
import java.io.ByteArrayOutputStream
import java.io.Closeable
import java.io.DataOutputStream
import java.io.OutputStream
import java.security.MessageDigest
import java.util.UUID

/**
 * Container writer of RCFile, symmetric with [RcFileReader].
 *
 * Where a cell's bytes come from (RCTEXT text or RCBINARY LazyBinary) is not its business. It only writes uncompressed files:
 * Hive reads uncompressed RCFiles without any problem, while compression needs one compression stream per column plus two
 * lengths recorded in the key — far less worthwhile than simply switching to ORC.
 *
 * @author wuya
 */
class RcFileWriter(output: OutputStream, private val columnCount: Int) : Closeable {

    private val counting = CountingOutputStream(output)
    private val out = DataOutputStream(counting)
    private val sync = generateSync()
    private var lastSyncPosition = 0L

    /** Data buffer and "per-row length" buffer (run-length encoded) of each column. */
    private val columns = Array(columnCount) { ColumnBuffer() }
    private var bufferedRows = 0
    private var bufferedBytes = 0

    init {
        require(columnCount > 0) { "an RCFile must have at least one column" }
        writeHeader()
    }

    private fun writeHeader() {
        out.write(RcFile.MAGIC)
        out.writeByte(RcFile.CURRENT_VERSION.toInt())
        out.writeBoolean(false)
        // The column count must be written into the metadata; reading depends entirely on it
        out.writeInt(1)
        RcFile.writeText(out, RcFile.COLUMN_NUMBER_METADATA_KEY)
        RcFile.writeText(out, columnCount.toString())
        out.write(sync)
        out.flush()
        lastSyncPosition = counting.count
    }

    /** Appends one row. The length of [cells] must equal the column count; null means that cell is empty. */
    fun append(cells: Array<ByteArray?>) {
        require(cells.size == columnCount) { "a row gave ${cells.size} columns, the table has $columnCount" }
        cells.forEachIndexed { index, cell ->
            columns[index].append(cell ?: EMPTY)
            bufferedBytes += cell?.size ?: 0
        }
        bufferedRows++
        if (bufferedBytes >= RcFile.RECORD_BUFFER_SIZE) {
            flushRecord()
        }
    }

    private fun flushRecord() {
        if (bufferedRows == 0) {
            return
        }
        columns.forEach { it.flushGroup() }

        val key = ByteArrayOutputStream(64)
        DataOutputStream(key).use { keyOut ->
            WritableUtils.writeVLong(keyOut, bufferedRows.toLong())
            columns.forEach { column ->
                val dataLength = column.data.size().toLong()
                WritableUtils.writeVLong(keyOut, dataLength)
                // Uncompressed, so the "compressed length" and the "uncompressed length" are the same number
                WritableUtils.writeVLong(keyOut, dataLength)
                WritableUtils.writeVLong(keyOut, column.lengths.size().toLong())
                keyOut.write(column.lengths.toByteArray())
            }
        }
        val keyBytes = key.toByteArray()
        val valueLength = columns.sumOf { it.data.size() }

        writeSyncIfNeeded()
        out.writeInt(keyBytes.size + valueLength)
        out.writeInt(keyBytes.size)
        // Uncompressed writes the uncompressed length once more here; compressed would write the compressed length
        out.writeInt(keyBytes.size)
        out.write(keyBytes)
        columns.forEach { out.write(it.data.toByteArray()) }

        columns.forEach { it.clear() }
        bufferedRows = 0
        bufferedBytes = 0
    }

    /** Inserts a sync block every [RcFile.SYNC_INTERVAL] bytes; the read side uses it for byte-range splitting. */
    private fun writeSyncIfNeeded() {
        out.flush()
        if (counting.count >= lastSyncPosition + RcFile.SYNC_INTERVAL) {
            out.writeInt(RcFile.SYNC_ESCAPE)
            out.write(sync)
            out.flush()
            lastSyncPosition = counting.count
        }
    }

    override fun close() {
        flushRecord()
        out.flush()
        out.close()
    }

    private class ColumnBuffer {
        val data = ByteArrayOutputStream(1024)
        val lengths = ByteArrayOutputStream(64)

        private var runLength = 0
        private var previousLength = -1

        fun append(cell: ByteArray) {
            data.write(cell)
            val length = cell.size
            when {
                previousLength < 0 -> startGroup(length)
                length != previousLength -> {
                    flushGroup()
                    startGroup(length)
                }

                else -> runLength++
            }
        }

        private fun startGroup(length: Int) {
            previousLength = length
            runLength = 0
        }

        /** Run-length encoding: write the length first, then a `~repeat count` when it repeats. */
        fun flushGroup() {
            if (previousLength < 0) {
                return
            }
            DataOutputStream(lengths).use { out ->
                WritableUtils.writeVLong(out, previousLength.toLong())
                if (runLength > 0) {
                    WritableUtils.writeVLong(out, runLength.inv().toLong())
                }
            }
            runLength = -1
            previousLength = -1
        }

        fun clear() {
            data.reset()
            lengths.reset()
            previousLength = -1
            runLength = 0
        }
    }

    /** Only there to know which byte we are at — sync block intervals are counted in bytes. */
    private class CountingOutputStream(private val delegate: OutputStream) : OutputStream() {
        var count: Long = 0
            private set

        override fun write(b: Int) {
            delegate.write(b)
            count++
        }

        override fun write(b: ByteArray, off: Int, len: Int) {
            delegate.write(b, off, len)
            count += len
        }

        override fun flush() = delegate.flush()

        override fun close() = delegate.close()
    }

    private companion object {
        val EMPTY = ByteArray(0)

        /** The sync marker must be unique within the file; Hive also uses the MD5 of a UUID plus a timestamp. */
        fun generateSync(): ByteArray {
            val seed = "${UUID.randomUUID()}@${System.currentTimeMillis()}"
            return MessageDigest.getInstance("MD5").digest(seed.toByteArray(Charsets.UTF_8))
        }
    }
}
