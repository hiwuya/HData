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
 * Reader for SEQUENCEFILE.
 *
 * Hive's SequenceFile tables only use SequenceFile as a **container**: the key is usually empty
 * and the value holds one line of text, encoded exactly like TEXTFILE (`LazySimpleSerDe`),
 * so [LazySimpleCodec] is reused directly here.
 *
 * The claimable boundary is the **sync block**: `SequenceFile.Reader.sync(position)` locates the first sync point after a
 * position. A claimed offset is committed only when it advances — in block-compressed files several consecutive rows report the
 * same `getPosition()`, and claiming the same offset twice makes `OffsetRangeTracker` treat it as a violation and throw.
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

        // Writables such as NullWritable have a private constructor (the singleton is only reachable through get()), so only
        // Hadoop's own ReflectionUtils can handle them; newInstance() cannot be called directly
        val key = ReflectionUtils.newInstance(sequenceReader.keyClass, configuration) as Writable
        val value = ReflectionUtils.newInstance(sequenceReader.valueClass, configuration) as Writable
        if (range.from > 0) {
            sequenceReader.sync(range.from)
        }
        var claimed = -1L
        while (true) {
            // One sync block is an indivisible unit: the records inside a block can only be read sequentially after the block's
            // leading sync marker, they cannot be recovered from an arbitrary byte in the middle. So claim by **block** — whichever
            // range the block start offset falls in owns the whole block, and once claimed the whole block is read. Neighbouring
            //
            // This also sidesteps the block-compression pitfall (see the contract in HiveRecordReader): in block-compressed files
            // consecutive rows report the same `getPosition()`, so claiming per row triggers an OffsetRangeTracker violation;
            val blockStart = sequenceReader.position.coerceAtLeast(range.from)
            if (blockStart > claimed) {
                if (!claim.tryClaim(blockStart)) {
                    return false
                }
                claimed = blockStart
            }
            // Read the first record inside the block (it counts as this block's whether or not it just crossed a sync marker)
            if (!sequenceReader.next(key, value)) {
                return true
            }
            output(toRow(decode(value)))
            // Read the remaining records of the block: if the next next() crosses the sync marker at the end of the block, that
            // record is the first of the next block, reported by syncSeen(), so stop and let the outer loop claim the next block.
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
