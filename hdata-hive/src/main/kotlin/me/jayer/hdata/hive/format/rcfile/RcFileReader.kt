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
 * RCFile 的容器读取器：解析文件头、按同步标记定位、逐个记录读出"行组"。
 *
 * 列的字节怎么解释（RCTEXT 的文本编码还是 RCBINARY 的 LazyBinary 编码）不归它管，
 * 那是上层 [me.jayer.hdata.hive.format.RcFileRecordReader] 的事。
 *
 * @author wuya
 */
class RcFileReader(
    private val input: FSDataInputStream,
    private val fileLength: Long,
    private val configuration: Configuration,
) : Closeable {

    /** 文件里有多少列，从元数据 `hive.io.rcfile.column.number` 取。 */
    var columnCount: Int = 0
        private set

    private val sync = ByteArray(RcFile.SYNC_HASH_SIZE)
    private var headerEnd: Long = 0
    private var codec: CompressionCodec? = null

    /** 当前读到的字节位置，也就是下一个记录的起始偏移量。 */
    val position: Long get() = input.pos

    init {
        readHeader()
    }

    private fun readHeader() {
        val magic = ByteArray(RcFile.MAGIC.size)
        input.readFully(magic)
        require(!magic.contentEquals(RcFile.LEGACY_MAGIC)) {
            "这是 Hive 0.7 之前的老式 RCFile（SEQ 魔数），暂不支持，用 Hive 重写一遍即可"
        }
        require(magic.contentEquals(RcFile.MAGIC)) {
            "不是 RCFile：魔数是 ${magic.joinToString(",")}"
        }
        val version = input.readByte()
        require(version <= RcFile.CURRENT_VERSION) {
            "RCFile 版本 $version 高于本实现支持的 ${RcFile.CURRENT_VERSION}"
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
        require(count >= 0) { "RCFile 元数据条数为负: $count" }
        repeat(count) {
            val key = RcFile.readText(input)
            val value = RcFile.readText(input)
            if (key == RcFile.COLUMN_NUMBER_METADATA_KEY) {
                columnCount = value.toInt()
            }
        }
        require(columnCount > 0) { "RCFile 元数据里没有 ${RcFile.COLUMN_NUMBER_METADATA_KEY}，无法确定列数" }
    }

    /**
     * 定位到 [target] 之后的第一个同步标记。
     *
     * 这是按字节区间并行读的入口：每个分片从自己区间内的第一个同步点开始，
     * 跨过区间起点的那个记录归上一个分片。逻辑对齐 Hive `RCFile.Reader.sync`。
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
        // 先把前缀填成一个绝不会命中同步标记的值，避免跨缓冲区的误匹配
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
     * 读下一个行组。
     *
     * @return 没有更多记录时返回 null
     */
    fun nextBlock(): RcFileBlock? {
        val recordLength = readRecordLength() ?: return null
        val keyLength = input.readInt()
        val writtenKeyLength = input.readInt()
        val key = readKey(keyLength, writtenKeyLength)
        val valueLength = recordLength - keyLength
        require(valueLength >= 0) { "RCFile 记录长度不合法: recordLength=$recordLength keyLength=$keyLength" }

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

    /** 记录长度前面可能插着一个同步块，读到就跳过。 */
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
            require(check.contentEquals(sync)) { "RCFile 同步标记对不上，文件已损坏" }
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

    /** 每一列（以及 key）都是各自独立压缩的，解压时按列各来一遍。 */
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
                    throw EOFException("RCFile 解压后长度不足: 期望 $uncompressedLength，实际 $read")
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

/** 一个行组：每列的数据是连着放的，靠 [RcFileColumn.cellLengths] 切开。 */
class RcFileBlock(val rowCount: Int, val columns: Array<RcFileColumn>)

/**
 * 一列在一个行组里的全部数据。
 *
 * [cellLengths] 是**游程编码**的：先写一个长度，如果紧接着的若干行长度相同，
 * 再写一个 `~重复次数`（负数）表示"上一个长度再重复这么多次"。
 * 定长列（int、timestamp）因此只占几个字节。
 */
class RcFileColumn(val data: ByteArray, private val cellLengths: ByteArray) {

    private var lengthOffset = 0
    private var dataOffset = 0
    private var runLength = 0
    private var previousLength = -1

    /** 本列在这个行组里是不是全空。 */
    val allNull: Boolean get() = cellLengths.isEmpty()

    /** 前进到下一行，返回这一行的数据区间。 */
    fun nextCell(): IntRange {
        if (runLength > 0) {
            runLength--
        } else {
            val length = RcFile.readVLong(cellLengths, lengthOffset)
            lengthOffset += length.length
            if (length.value < 0) {
                // 读到的是游程：用上一个长度，再重复 (~value) - 1 次
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
