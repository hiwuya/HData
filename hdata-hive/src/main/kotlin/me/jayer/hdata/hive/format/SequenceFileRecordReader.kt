package me.jayer.hdata.hive.format

import me.jayer.hdata.hive.split.HiveFile
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hadoop.util.ReflectionUtils

/**
 * SEQUENCEFILE 的读取器。
 *
 * Hive 的 SequenceFile 表只是拿 SequenceFile 当**容器**：key 一般是空的，
 * value 里放的就是一行文本，编码规则和 TEXTFILE 完全一样（`LazySimpleSerDe`），
 * 所以这里直接复用 [LazySimpleCodec]。
 *
 * 可认领的边界是**同步块**：`SequenceFile.Reader.sync(position)` 会定位到该位置之后的第一个同步点。
 * 认领的偏移量只在推进时才提交——块压缩的文件里连续多行的 `getPosition()` 是同一个值，
 * 重复认领同一个偏移量会被 `OffsetRangeTracker` 判成违约直接抛异常。
 *
 * @author wuya
 */
class SequenceFileRecordReader(
    private val file: HiveFile,
    private val range: OffsetRange,
    spec: HiveReadSpec,
    partitionValues: List<Any?>,
    serdeParameters: Map<String, String>,
    private val configuration: Configuration,
) : HiveRecordReader(spec, partitionValues) {

    private val codec = LazySimpleCodec(serdeParameters)
    private val fieldTypes = spec.dataFieldTypes
    private var reader: SequenceFile.Reader? = null

    override fun read(claim: OffsetClaim, output: (Row) -> Unit): Boolean {
        val sequenceReader = SequenceFile.Reader(configuration, SequenceFile.Reader.file(Path(file.path)))
        reader = sequenceReader

        // NullWritable 这类 Writable 的构造器是私有的（只能通过 get() 拿单例），
        // 只有 Hadoop 自己的 ReflectionUtils 处理得了，不能直接 newInstance()
        val key = ReflectionUtils.newInstance(sequenceReader.keyClass, configuration) as Writable
        val value = ReflectionUtils.newInstance(sequenceReader.valueClass, configuration) as Writable
        if (range.from > 0) {
            sequenceReader.sync(range.from)
        }
        var claimed = -1L
        while (true) {
            // 一个同步块（block）是一个不可分割的单元：块里的记录只能从块首的 sync 标记之后顺序读，
            // 不能从块中间任意字节恢复。所以按**块**认领——块首偏移量落在哪个区间，整块就归哪个分片，
            // 一旦认领就把整块读完。这样相邻分片既不重也不漏。
            //
            // 这同时解决了块压缩的坑（见 HiveRecordReader 的约定）：块压缩文件里连续多行
            // `getPosition()` 是同一个值，逐行认领会触发 OffsetRangeTracker 的违约；按块认领则不会。
            val blockStart = sequenceReader.position.coerceAtLeast(range.from)
            if (blockStart > claimed) {
                if (!claim.tryClaim(blockStart)) {
                    return false
                }
                claimed = blockStart
            }
            // 读块内的第一条记录（无论它是否刚跨过 sync 标记都算本块）
            if (!sequenceReader.next(key, value)) {
                return true
            }
            output(toRow(decode(value)))
            // 读块内剩余记录：下一次 next() 若跨过了块末尾的 sync 标记，那条记录就是下一个块的首条，
            // 由 syncSeen() 报出来，停下来交给外层去认领下一个块。
            while (true) {
                if (!sequenceReader.next(key, value)) {
                    return true
                }
                if (sequenceReader.syncSeen()) {
                    break
                }
                output(toRow(decode(value)))
            }
        }
    }

    private fun decode(value: Writable): Array<Any?> {
        val line = when (value) {
            is Text -> value.toString()
            is BytesWritable -> String(value.bytes, 0, value.length, codec.charset)
            else -> value.toString()
        }
        return codec.decodeRow(line, fieldTypes, spec.projectedDataIndexes)
    }

    override fun close() {
        reader?.close()
        reader = null
    }
}
