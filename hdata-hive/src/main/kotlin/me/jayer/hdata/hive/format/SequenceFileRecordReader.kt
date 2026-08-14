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
            val position = sequenceReader.position.coerceAtLeast(range.from)
            if (position > claimed) {
                if (!claim.tryClaim(position)) {
                    return false
                }
                claimed = position
            }
            if (!sequenceReader.next(key, value)) {
                return true
            }
            output(toRow(decode(value)))
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
