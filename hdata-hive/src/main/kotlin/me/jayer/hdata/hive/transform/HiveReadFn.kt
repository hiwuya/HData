package me.jayer.hdata.hive.transform

import me.jayer.hdata.hive.format.HiveReadSpec
import me.jayer.hdata.hive.format.HiveRecordReaders
import me.jayer.hdata.hive.format.HiveStorageFormat
import me.jayer.hdata.hive.format.OffsetClaim
import me.jayer.hdata.hive.format.PredicateEvaluator
import me.jayer.hdata.hive.format.samplingSeed
import me.jayer.hdata.hive.split.HiveFile
import me.jayer.hdata.hive.SampleMethod
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
 * A splittable DoFn that reads one Hive data file in parallel by **byte range**.
 *
 * The restriction is a byte range `[from, to)` inside the file, and the claim granularity depends on the format: stripes for
 * ORC, row groups for Parquet, sync blocks for Avro / SequenceFile / RCFile, lines for text (see [me.jayer.hdata.hive.format.HiveRecordReader]).
 * This way a 10GB ORC file can be read by several workers at once, and at runtime the remaining work can still be redistributed
 * to idle workers halfway through.
 *
 * Non-splittable files (whole-file gzip text, quoted CSV, tables configured to skip header/footer lines) use a single logical restriction `[0, 1)`,
 * so parallelism can only come from the number of files; this also makes it impossible for the runtime to split off a residual byte range.
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
    fun getInitialRestriction(@Element file: HiveFile): OffsetRange =
        initialRestriction(file.length, isSplittable(file))

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
        val byteSplittable = isSplittable(file)
        val readerRange: OffsetRange
        val claim: OffsetClaim
        if (byteSplittable) {
            readerRange = range
            claim = OffsetClaim { tracker.tryClaim(it) }
        } else {
            // A non-splittable format has exactly one logical work unit, `[0,1)`. Claim it first, then let the reader advance on its own
            // over the real whole-file range; the file offset must not be handed to the logical tracker, otherwise the position of the second record is guaranteed to pass 1.
            if (!tracker.tryClaim(range.from)) return
            readerRange = OffsetRange(0, file.length.coerceAtLeast(1))
            claim = OffsetClaim { true }
        }
        var count = 0L
        // Row-level safety-net filtering: predicate pushdown can skip whole stripes/row groups in ORC/Parquet, but the rows that
        // remain, and the rows of other formats, must still be filtered again by the predicates to keep the result correct.
        val predicates = spec.predicates
        // Sampling pushdown (Trino's TABLESAMPLE BERNOULLI equivalent): each row is kept with probability sampleFraction, done
        // right here in this DoFn (a real pushdown, dropped rows never reach downstream). The random sequence mixes in the file
        // and the restriction start, so parallel splits do not all repeat the same sequence; the job-level seed is fixed at graph
        // construction time, so worker retries do not drift. Row-level sampling (BERNOULLI) happens in this DoFn; block sampling (SYSTEM) is left to each format's reader, which skips by stripe/row group.
        val doRowSample = spec.sampleFraction < 1.0 && spec.sampleMethod == SampleMethod.BERNOULLI
        val sampler = if (doRowSample) {
            Random(samplingSeed(checkNotNull(spec.sampleSeed), file.path, range.from))
        } else null
        val output: (Row) -> Unit = outputLabel@{ row ->
            if (predicates.isNotEmpty() && !PredicateEvaluator.matches(row, predicates)) return@outputLabel
            if (doRowSample && sampler!!.nextDouble() >= spec.sampleFraction) return@outputLabel
            receiver.output(row)
            count++
        }
        HiveRecordReaders.open(file, readerRange, spec, configuration).use { reader ->
            val completed = reader.read(claim) { row ->
                output(row)
            }
            if (completed) {
                // The data is exhausted, so make one extra out-of-range claim: OffsetRangeTracker's checkDone() requires the offset
                // of the last *attempt* to be >= to - 1, and skipping this step reports
                // "claiming work in [x, y) was not attempted".
                // Conversely, when a claim is rejected you **must not** add another, otherwise it throws because "the claimed
                tracker.tryClaim(tracker.currentRestriction().to)
            }
        }
        RECORDS_READ.inc(count)
        LOGGER.info("Hive file[{}] range [{}, {}) read {} rows", file.path, range.from, range.to, count)
    }

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = restriction.newTracker()

    @GetRestrictionCoder
    fun restrictionCoder(): Coder<OffsetRange> = OffsetRange.Coder()

    /**
     * Whether the file can be split by byte.
     *
     * Besides format and compression (see [HiveFileSystems.isSplittable]), a table configured to skip header/footer lines must
     * also be read as a whole file: `skip.header.line.count` can only know which lines to skip when reading from the start of the
     * file, and `skip.footer.line.count` has to reach the end of the file to decide. A one-line header is still splittable — the
     * first split starts at 0, so it skips that line naturally.
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

        internal fun initialRestriction(fileLength: Long, splittable: Boolean): OffsetRange =
            if (splittable) OffsetRange(0, fileLength.coerceAtLeast(1)) else OffsetRange(0, 1)
    }
}
