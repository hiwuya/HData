package me.jayer.hdata.hive.transform

import me.jayer.hdata.hive.format.HiveReadSpec
import me.jayer.hdata.hive.format.HiveRecordReaders
import me.jayer.hdata.hive.format.HiveStorageFormat
import me.jayer.hdata.hive.format.OffsetClaim
import me.jayer.hdata.hive.format.PredicateEvaluator
import me.jayer.hdata.hive.split.HiveFile
import me.jayer.hdata.hive.split.HiveFileSystems
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory
import java.util.Random

/**
 * 按**字节区间**并行读一个 Hive 数据文件的 Splittable DoFn。
 *
 * 限制是文件内的字节区间 `[from, to)`，认领的粒度由格式决定：ORC 是 stripe、Parquet 是 row group、
 * Avro / SequenceFile / RCFile 是同步块、文本是行（见 [me.jayer.hdata.hive.format.HiveRecordReader]）。
 * 这样一个 10GB 的 ORC 文件也能被多个 worker 分着读，而且运行时还能在读到一半时
 * 把剩下的活重新分配给空闲 worker。
 *
 * 不可切分的文件（整文件 gzip 的文本、带引号的 CSV、带表头/表尾行数配置的表）
 * 在 [splitRestriction] 里原样输出一个区间，并行度只能来自文件个数。
 *
 * @author wuya
 */
@DoFn.BoundedPerElement
class HiveReadFn(
    private val spec: HiveReadSpec,
    private val hadoopConf: Map<String, String>,
    private val splitBytes: Long,
) : DoFn<HiveFile, Row>() {

    @GetInitialRestriction
    fun getInitialRestriction(@Element file: HiveFile): OffsetRange = OffsetRange(0, file.length.coerceAtLeast(1))

    @SplitRestriction
    fun splitRestriction(
        @Element file: HiveFile,
        @Restriction restriction: OffsetRange,
        receiver: OutputReceiver<OffsetRange>,
    ) {
        if (restriction.to <= restriction.from) {
            return
        }
        if (!isSplittable(file)) {
            receiver.output(restriction)
            return
        }
        restriction.split(splitBytes, splitBytes / 2).forEach { receiver.output(it) }
    }

    @ProcessElement
    fun processElement(
        @Element file: HiveFile,
        tracker: RestrictionTracker<OffsetRange, Long>,
        receiver: OutputReceiver<Row>,
    ) {
        val range = tracker.currentRestriction()
        if (range.to <= range.from) {
            return
        }
        val configuration = HiveFileSystems.configurationOf(hadoopConf)
        var count = 0L
        // 行级兜底过滤：谓词下推在 ORC/Parquet 上能跳过整段 stripe/row group，
        // 但跳剩下的行、以及其它格式的行仍要按谓词再筛一遍，保证结果正确。
        val predicates = spec.predicates
        // 采样下推（对标 Trino 的 TABLESAMPLE BERNOULLI）：每行以 sampleFraction 的概率被保留，
        // 直接在本 DoFn 里完成（真正的下推，被丢掉的行不会发到下游）。同一 DoFn 实例内用同一把 Random，
        // 所以每个文件内的命中位置是确定的；跨文件相互独立，整体保留比例≈sampleFraction。
        val doSample = spec.sampleFraction < 1.0
        val sampler = if (doSample) Random(spec.sampleSeed ?: System.nanoTime()) else null
        val output: (Row) -> Unit = outputLabel@{ row ->
            if (predicates.isNotEmpty() && !PredicateEvaluator.matches(row, predicates)) return@outputLabel
            if (doSample && sampler!!.nextDouble() >= spec.sampleFraction) return@outputLabel
            receiver.output(row)
            count++
        }
        HiveRecordReaders.open(file, range, spec, configuration).use { reader ->
            val completed = reader.read(OffsetClaim {
                tracker.tryClaim(it)
            }) { row ->
                output(row)
            }
            if (completed) {
                // 数据读完了，补一次越界认领：OffsetRangeTracker 的 checkDone() 要求
                // 最后一次*尝试*的偏移量 >= to - 1，漏了这一步会报
                // "claiming work in [x, y) was not attempted"。
                // 反过来，认领被拒绝时**不能**再补，否则会因为"认领的偏移量小于上一次尝试"直接抛异常
                tracker.tryClaim(tracker.currentRestriction().to)
            }
        }
        RECORDS_READ.inc(count)
        LOGGER.info("Hive 文件[{}] 区间 [{}, {}) 读出 {} 行", file.path, range.from, range.to, count)
    }

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = restriction.newTracker()

    @GetRestrictionCoder
    fun restrictionCoder(): Coder<OffsetRange> = OffsetRange.Coder()

    /**
     * 能不能按字节切。
     *
     * 除了格式与压缩（见 [HiveFileSystems.isSplittable]），表属性里配了要跳过表头/表尾行数时
     * 也必须整文件读：`skip.header.line.count` 只有从文件开头读才知道该跳哪几行，
     * `skip.footer.line.count` 更是要读到文件末尾才能确定。
     * 表头恰好只有一行时可以切——第一个分片从 0 开始，天然会跳掉它。
     */
    private fun isSplittable(file: HiveFile): Boolean {
        if (spec.headerLineCount > 1 || spec.footerLineCount > 0) {
            return false
        }
        val format = HiveStorageFormat.of(file.partition.storage.storageFormat)
        return HiveFileSystems.isSplittable(format, file.path, HiveFileSystems.configurationOf(hadoopConf))
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(HiveReadFn::class.java)
        private val RECORDS_READ = Metrics.counter(HiveReadFn::class.java, "records_read")
    }
}
