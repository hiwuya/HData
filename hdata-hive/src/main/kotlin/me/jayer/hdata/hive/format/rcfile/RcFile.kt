package me.jayer.hdata.hive.format.rcfile

import org.apache.hadoop.io.WritableUtils
import java.io.DataInput
import java.io.DataOutput

/**
 * Container format constants and shared utilities of RCFile.
 *
 * This layer is **implemented by us**, without pulling in `hive-exec`: the `RCFile` class sits inside the 80MB hive-exec jar,
 * which bundles unrelocated copies of avro / orc / parquet / protobuf / thrift, so including it would inevitably clash with the
 * versions we declare. Trino makes the same choice (`trino-hive-formats` ships its own RCFile reader and writer).
 *
 * The format itself (character by character with Hive's `org.apache.hadoop.hive.ql.io.RCFile`):
 *
 * ```
 * file header: "RCF" + version(1) + boolean(compressed?) [+ compression codec class name] + metadata + 16-byte sync marker
 * record:      [sync block: int(-1) + 16-byte sync marker]     // inserted every SYNC_INTERVAL bytes written
 *         int  total record length = key length + value length
 *         int  uncompressed key length
 *         int  key length actually written (the same as the previous one when uncompressed)
 *         key bytes
 *         value bytes = the data of each column concatenated in order
 * key layout: VLong row count
 *          per column: VLong length of this column inside the value area
 *                      VLong uncompressed length of this column
 *                      VLong byte count of this column's "per-row length buffer"
 *                      per-row length buffer (run-length encoded, see [RcFileColumn])
 * ```
 *
 * @author wuya
 */
object RcFile {

    /** Magic of the new format; the old one is `SEQ` + version 6. */
    val MAGIC = byteArrayOf('R'.code.toByte(), 'C'.code.toByte(), 'F'.code.toByte())

    val LEGACY_MAGIC = byteArrayOf('S'.code.toByte(), 'E'.code.toByte(), 'Q'.code.toByte())

    const val CURRENT_VERSION: Byte = 1

    /** Length of the sync marker. */
    const val SYNC_HASH_SIZE = 16

    /** Total length of a sync block: 4 escape bytes + 16 marker bytes. */
    const val SYNC_SIZE = 4 + SYNC_HASH_SIZE

    /** Insert a sync block every this many bytes written, consistent with Hive's `SYNC_INTERVAL`. */
    const val SYNC_INTERVAL = 100 * SYNC_SIZE

    /** Start marker of a sync block, also a record length that can never occur. */
    const val SYNC_ESCAPE = -1

    /** The column count is recorded in the file metadata and must be read from there. */
    const val COLUMN_NUMBER_METADATA_KEY = "hive.io.rcfile.column.number"

    /** Flush a record once this many bytes have accumulated, matching Hive's `hive.io.rcfile.record.buffer.size` default. */
    const val RECORD_BUFFER_SIZE = 4 * 1024 * 1024

    /**
     * String encoding of Hadoop `Text`: VInt length + UTF-8 bytes.
     */
    fun writeText(out: DataOutput, value: String) {
        val bytes = value.toByteArray(Charsets.UTF_8)
        WritableUtils.writeVInt(out, bytes.size)
        out.write(bytes)
    }

    fun readText(input: DataInput): String {
        val length = WritableUtils.readVInt(input)
        val bytes = ByteArray(length)
        input.readFully(bytes)
        return String(bytes, Charsets.UTF_8)
    }

    /**
     * Reads a Hadoop VInt / VLong from a byte array.
     *
     * Hadoop's `WritableUtils` only offers a `DataInput`-based version, while here we need random access by offset into the
     * already in-memory "per-row length buffer", so we decode it ourselves. The rules match `WritableUtils.readVLong` exactly.
     *
     * @return the value and the number of bytes it occupies
     */
    fun readVLong(bytes: ByteArray, offset: Int): VLong {
        val firstByte = bytes[offset]
        val length = decodeVIntSize(firstByte)
        if (length == 1) {
            return VLong(firstByte.toLong(), 1)
        }
        var value = 0L
        for (i in 0 until length - 1) {
            value = (value shl 8) or (bytes[offset + 1 + i].toLong() and 0xFF)
        }
        return VLong(if (isNegativeVInt(firstByte)) value.inv() else value, length)
    }

    /** Consistent with `WritableUtils.decodeVIntSize`. */
    fun decodeVIntSize(value: Byte): Int = when {
        value >= -112 -> 1
        value < -120 -> -119 - value
        else -> -111 - value
    }

    /** Consistent with `WritableUtils.isNegativeVInt`. */
    private fun isNegativeVInt(value: Byte): Boolean = value < -120 || (value in -112..-1)

    data class VLong(val value: Long, val length: Int)
}
