package me.jayer.hdata.hive.format.rcfile

import org.apache.hadoop.io.WritableUtils
import java.io.DataInput
import java.io.DataOutput

/**
 * RCFile 的容器格式常量与共用工具。
 *
 * 这一层是**自己实现**的，没有引 `hive-exec`：`RCFile` 那个类躺在 80MB 的 hive-exec 里，
 * 而那个 jar 把 avro / orc / parquet / protobuf / thrift 各打了一份没有重定位的副本进去，
 * 拉进来必然和我们自己声明的版本打架。Trino 也是同样的选择（`trino-hive-formats` 里自带 RCFile 读写）。
 *
 * 格式本身（逐字对齐 Hive `org.apache.hadoop.hive.ql.io.RCFile`）：
 *
 * ```
 * 文件头: "RCF" + 版本号(1) + boolean(是否压缩) [+ 压缩编解码器类名] + 元数据 + 16 字节同步标记
 * 记录:   [同步块: int(-1) + 16 字节同步标记]     // 每写满 SYNC_INTERVAL 字节插一个
 *         int  记录总长 = key 长 + value 长
 *         int  key 未压缩长度
 *         int  key 实际写入长度（未压缩时与上一个相同）
 *         key 字节
 *         value 字节 = 各列的数据依次拼接
 * key 结构: VLong 行数
 *          每列: VLong 本列在 value 区的长度
 *                VLong 本列未压缩长度
 *                VLong 本列"每行长度缓冲"的字节数
 *                每行长度缓冲（游程编码，见 [RcFileColumn]）
 * ```
 *
 * @author wuya
 */
object RcFile {

    /** 新格式的魔数，老格式是 `SEQ` + 版本 6。 */
    val MAGIC = byteArrayOf('R'.code.toByte(), 'C'.code.toByte(), 'F'.code.toByte())

    val LEGACY_MAGIC = byteArrayOf('S'.code.toByte(), 'E'.code.toByte(), 'Q'.code.toByte())

    const val CURRENT_VERSION: Byte = 1

    /** 同步标记的长度。 */
    const val SYNC_HASH_SIZE = 16

    /** 同步块的总长度：4 字节转义 + 16 字节标记。 */
    const val SYNC_SIZE = 4 + SYNC_HASH_SIZE

    /** 每写满这么多字节插一个同步块，与 Hive 的 `SYNC_INTERVAL` 一致。 */
    const val SYNC_INTERVAL = 100 * SYNC_SIZE

    /** 同步块的起始标记，也是一个不可能出现的记录长度。 */
    const val SYNC_ESCAPE = -1

    /** 列数记在文件元数据里，读的时候必须从这里拿。 */
    const val COLUMN_NUMBER_METADATA_KEY = "hive.io.rcfile.column.number"

    /** 攒够这么多字节就刷一个记录出去，与 Hive 的 `hive.io.rcfile.record.buffer.size` 默认值一致。 */
    const val RECORD_BUFFER_SIZE = 4 * 1024 * 1024

    /**
     * Hadoop `Text` 的字符串编码：VInt 长度 + UTF-8 字节。
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
     * 从字节数组里读一个 Hadoop VInt / VLong。
     *
     * Hadoop 的 `WritableUtils` 只提供基于 `DataInput` 的版本，而这里要在已经读进内存的
     * "每行长度缓冲"上按偏移量随机读，所以自己解一遍。编码规则与 `WritableUtils.readVLong` 完全一致。
     *
     * @return 值与它占用的字节数
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

    /** 与 `WritableUtils.decodeVIntSize` 一致。 */
    fun decodeVIntSize(value: Byte): Int = when {
        value >= -112 -> 1
        value < -120 -> -119 - value
        else -> -111 - value
    }

    /** 与 `WritableUtils.isNegativeVInt` 一致。 */
    private fun isNegativeVInt(value: Byte): Boolean = value < -120 || (value in -112..-1)

    data class VLong(val value: Long, val length: Int)
}
