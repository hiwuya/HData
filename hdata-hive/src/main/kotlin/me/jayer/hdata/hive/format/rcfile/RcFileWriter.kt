package me.jayer.hdata.hive.format.rcfile

import org.apache.hadoop.io.WritableUtils
import java.io.ByteArrayOutputStream
import java.io.Closeable
import java.io.DataOutputStream
import java.io.OutputStream
import java.security.MessageDigest
import java.util.UUID

/**
 * RCFile 的容器写入器，与 [RcFileReader] 对称。
 *
 * 单元格的字节怎么来（RCTEXT 的文本还是 RCBINARY 的 LazyBinary）不归它管。
 * 只写不压缩的文件：Hive 读不压缩的 RCFile 没有任何问题，而压缩要按列各起一个
 * 压缩流、还要在 key 里记两套长度，收益远不如直接换 ORC。
 *
 * @author wuya
 */
class RcFileWriter(output: OutputStream, private val columnCount: Int) : Closeable {

    private val counting = CountingOutputStream(output)
    private val out = DataOutputStream(counting)
    private val sync = generateSync()
    private var lastSyncPosition = 0L

    /** 每列的数据缓冲与"每行长度"缓冲（游程编码）。 */
    private val columns = Array(columnCount) { ColumnBuffer() }
    private var bufferedRows = 0
    private var bufferedBytes = 0

    init {
        require(columnCount > 0) { "RCFile 至少要有一列" }
        writeHeader()
    }

    private fun writeHeader() {
        out.write(RcFile.MAGIC)
        out.writeByte(RcFile.CURRENT_VERSION.toInt())
        out.writeBoolean(false)
        // 元数据里必须写列数，读的时候全靠它
        out.writeInt(1)
        RcFile.writeText(out, RcFile.COLUMN_NUMBER_METADATA_KEY)
        RcFile.writeText(out, columnCount.toString())
        out.write(sync)
        out.flush()
        lastSyncPosition = counting.count
    }

    /** 追加一行。[cells] 的长度必须等于列数，null 表示这一格是空的。 */
    fun append(cells: Array<ByteArray?>) {
        require(cells.size == columnCount) { "一行给了 ${cells.size} 列，表有 $columnCount 列" }
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
                // 不压缩，所以"压缩后长度"和"未压缩长度"是同一个数
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
        // 未压缩时这里再写一遍未压缩长度，压缩时写的是压缩后长度
        out.writeInt(keyBytes.size)
        out.write(keyBytes)
        columns.forEach { out.write(it.data.toByteArray()) }

        columns.forEach { it.clear() }
        bufferedRows = 0
        bufferedBytes = 0
    }

    /** 每写满 [RcFile.SYNC_INTERVAL] 字节插一个同步块，读取端靠它做字节区间切分。 */
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

        /** 游程编码：先写长度，重复出现时补一个 `~重复次数`。 */
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

    /** 只是为了知道当前写到第几个字节——同步块的间隔按字节算。 */
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

        /** 同步标记要在文件里独一无二，Hive 用的也是 UUID + 时间戳的 MD5。 */
        fun generateSync(): ByteArray {
            val seed = "${UUID.randomUUID()}@${System.currentTimeMillis()}"
            return MessageDigest.getInstance("MD5").digest(seed.toByteArray(Charsets.UTF_8))
        }
    }
}
