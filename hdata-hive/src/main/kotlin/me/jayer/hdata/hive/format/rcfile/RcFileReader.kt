package me.jayer.hdata.hive.format.rcfile

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.WritableUtils
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.util.ReflectionUtils
import java.io.ByteArrayInputStream
import java.io.Closeable
import java.io.DataInputStream
import java.io.EOFException

/**
 * Container reader of RCFile: parses the file header, locates by sync marker, and reads "row groups" out record by record.
 *
 * How a column's bytes are interpreted (RCTEXT text encoding or RCBINARY LazyBinary encoding) is not its business — that is
 * [me.jayer.hdata.hive.format.RcFileRecordReader]'s job one level up.
 *
 * @author wuya
 */
class RcFileReader(
    private val input: FSDataInputStream,
    private val fileLength: Long,
    private val configuration: Configuration,
) : Closeable {

    /** Number of columns in the file, taken from the metadata key `hive.io.rcfile.column.number`. */
    var columnCount: Int = 0
        private set

    private val sync = ByteArray(RcFile.SYNC_HASH_SIZE)
    private var headerEnd: Long = 0
    private var codec: CompressionCodec? = null

    /** Current byte position, i.e. the start offset of the next record. */
    val position: Long get() = input.pos

    init {
        readHeader()
    }

    private fun readHeader() {
        val magic = ByteArray(RcFile.MAGIC.size)
        input.readFully(magic)
        require(!magic.contentEquals(RcFile.LEGACY_MAGIC)) {
            "this is an old-style RCFile from before Hive 0.7 (SEQ magic), not supported; rewrite it with Hive"
        }
        require(magic.contentEquals(RcFile.MAGIC)) {
            "not an RCFile: the magic is ${magic.joinToString(",")}"
        }
        val version = input.readByte()
        require(version <= RcFile.CURRENT_VERSION) {
            "RCFile version $version is higher than the ${RcFile.CURRENT_VERSION} this implementation supports"
        }
        val compressed = input.readBoolean()
        if (compressed) {
            val codecClass = RcFile.readText(input)
            codec = ReflectionUtils.newInstance(Class.forName(codecClass), configuration) as CompressionCodec
        }
        readMetadata()
        input.readFully(sync)
        headerEnd = input.pos
    }

    private fun readMetadata() {
        val count = input.readInt()
        require(count >= 0) { "RCFile metadata entry count is negative: $count" }
        repeat(count) {
            val key = RcFile.readText(input)
            val value = RcFile.readText(input)
            if (key == RcFile.COLUMN_NUMBER_METADATA_KEY) {
                columnCount = value.toInt()
            }
        }
        require(columnCount > 0) { "the RCFile metadata has no ${RcFile.COLUMN_NUMBER_METADATA_KEY}, cannot determine the column count" }
    }

    /**
     * Locates the first sync marker after [target].
     *
     * This is the entry point for parallel reads by byte range: every split starts at the first sync point inside its own range,
     * and the record crossing the start of the range belongs to the previous split. The logic mirrors Hive's `RCFile.Reader.sync`.
     */
    fun syncTo(target: Long) {
        if (target < headerEnd) {
            input.seek(headerEnd)
            return
        }
        if (target + RcFile.SYNC_SIZE >= fileLength) {
            input.seek(fileLength)
            return
        }
        input.seek(target + 4)
        val prefix = sync.size
        val window = 512
        val buffer = ByteArray(prefix + window)
        // First fill the prefix with a value that can never match the sync marker, to avoid false matches across buffer boundaries
        buffer.fill((sync[0].toInt().inv()).toByte(), 0, prefix)
        while (true) {
            val position = input.pos
            val n = minOf(window.toLong(), fileLength - position).toInt()
            if (n <= 0) {
                input.seek(fileLength)
                return
            }
            input.readFully(buffer, prefix, n)
            for (i in 0 until n) {
                var j = 0
                while (j < sync.size && sync[j] == buffer[i + j]) {
                    j++
                }
                if (j == sync.size) {
                    input.seek(position + i - RcFile.SYNC_SIZE)
                    return
                }
            }
            System.arraycopy(buffer, buffer.size - prefix, buffer, 0, prefix)
        }
    }

    /**
     * Reads the next row group.
     *
     * @return null when there are no more records
     */
    fun nextBlock(): RcFileBlock? {
        val recordLength = readRecordLength() ?: return null
        val keyLength = input.readInt()
        val writtenKeyLength = input.readInt()
        val key = readKey(keyLength, writtenKeyLength)
        val valueLength = recordLength - keyLength
        require(valueLength >= 0) { "RCFile record length is invalid: recordLength=$recordLength keyLength=$keyLength" }

        val columns = Array(columnCount) { index ->
            val length = key.columnValueLengths[index]
            val bytes = ByteArray(length)
            input.readFully(bytes)
            RcFileColumn(
                data = decompress(bytes, key.columnUncompressedLengths[index]),
                cellLengths = key.columnCellLengthBuffers[index],
            )
        }
        return RcFileBlock(key.rowCount, columns)
    }

    /** A sync block may sit in front of the record length; skip it when one is found. */
    private fun readRecordLength(): Int? {
        if (input.pos >= fileLength) {
            return null
        }
        var length = try {
            input.readInt()
        } catch (e: EOFException) {
            return null
        }
        if (length == RcFile.SYNC_ESCAPE) {
            val check = ByteArray(RcFile.SYNC_HASH_SIZE)
            input.readFully(check)
            require(check.contentEquals(sync)) { "RCFile sync marker mismatch, the file is corrupt" }
            if (input.pos >= fileLength) {
                return null
            }
            length = input.readInt()
        }
        return length
    }

    private fun readKey(keyLength: Int, writtenKeyLength: Int): RcFileKey {
        val raw = ByteArray(writtenKeyLength)
        input.readFully(raw)
        val bytes = if (codec == null) raw else decompress(raw, keyLength)
        DataInputStream(ByteArrayInputStream(bytes)).use { keyIn ->
            val rowCount = WritableUtils.readVLong(keyIn).toInt()
            val valueLengths = IntArray(columnCount)
            val uncompressedLengths = IntArray(columnCount)
            val cellBuffers = Array(columnCount) { ByteArray(0) }
            for (i in 0 until columnCount) {
                valueLengths[i] = WritableUtils.readVLong(keyIn).toInt()
                uncompressedLengths[i] = WritableUtils.readVLong(keyIn).toInt()
                val bufferLength = WritableUtils.readVLong(keyIn).toInt()
                val buffer = ByteArray(bufferLength)
                keyIn.readFully(buffer)
                cellBuffers[i] = buffer
            }
            return RcFileKey(rowCount, valueLengths, uncompressedLengths, cellBuffers)
        }
    }

    /** Every column (and the key) is compressed independently, so decompression runs once per column. */
    private fun decompress(bytes: ByteArray, uncompressedLength: Int): ByteArray {
        val compressionCodec = codec ?: return bytes
        if (bytes.isEmpty()) {
            return bytes
        }
        val result = ByteArray(uncompressedLength)
        compressionCodec.createInputStream(ByteArrayInputStream(bytes)).use { stream ->
            var read = 0
            while (read < uncompressedLength) {
                val n = stream.read(result, read, uncompressedLength - read)
                if (n < 0) {
                    throw EOFException("RCFile decompressed length is insufficient: expected $uncompressedLength, got $read")
                }
                read += n
            }
        }
        return result
    }

    override fun close() {
        input.close()
    }
}

private class RcFileKey(
    val rowCount: Int,
    val columnValueLengths: IntArray,
    val columnUncompressedLengths: IntArray,
    val columnCellLengthBuffers: Array<ByteArray>,
)

/** One row group: the data of every column is contiguous and is cut apart by [RcFileColumn.cellLengths]. */
class RcFileBlock(val rowCount: Int, val columns: Array<RcFileColumn>)

/**
 * All data of one column inside one row group.
 *
 * [cellLengths] is **run-length encoded**: a length is written first, and if several following rows share that length a
 * `~repeat count` (negative) is written to mean "repeat the previous length this many more times". Fixed-length columns (int,
 * timestamp) therefore take only a few bytes.
 */
class RcFileColumn(val data: ByteArray, private val cellLengths: ByteArray) {

    private var lengthOffset = 0
    private var dataOffset = 0
    private var runLength = 0
    private var previousLength = -1

    /** Whether this column is entirely empty in this row group. */
    val allNull: Boolean get() = cellLengths.isEmpty()

    /** Advances to the next row, returning the data range of that row. */
    fun nextCell(): IntRange {
        if (runLength > 0) {
            runLength--
        } else {
            val length = RcFile.readVLong(cellLengths, lengthOffset)
            lengthOffset += length.length
            if (length.value < 0) {
                // A run was read: reuse the previous length and repeat it (~value) - 1 more times
                runLength = (length.value.inv()).toInt() - 1
            } else {
                previousLength = length.value.toInt()
                runLength = 0
            }
        }
        val start = dataOffset
        dataOffset += previousLength
        return start until dataOffset
    }
}
