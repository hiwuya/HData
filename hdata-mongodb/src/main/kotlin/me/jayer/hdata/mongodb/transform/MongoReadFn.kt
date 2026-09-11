package me.jayer.hdata.mongodb.transform

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import me.jayer.hdata.mongodb.MongoRowCodec
import me.jayer.hdata.mongodb.internal.MongoBuckets
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.values.Row
import org.bson.Document
import org.slf4j.LoggerFactory
import java.io.Serializable

/**
 * A single read task: one collection, plus the partition filter conditions it was split into by [MongoBuckets].
 *
 * Partition boundaries are fixed at graph-construction time and shipped with the element, so every worker sees
 * exactly the same boundaries.
 */
data class MongoReadSplit(
    val database: String,
    val collection: String,
    /** One complete filter condition (extended JSON) per partition, non-overlapping. */
    val partitionFilters: List<String>,
) : Serializable {
    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * Splittable DoFn that reads MongoDB in parallel by `_id` range.
 *
 * The restriction is the partition index range `[from, to)`, and `@ProcessElement` **claims partitions one at a time**:
 *
 * ```kotlin
 * while (index < end) {
 *     if (!tracker.tryClaim(index)) return
 *     readPartition(index)
 * }
 * ```
 *
 * This is fundamentally different from the pre-refactor `tryClaim(range.to - 1)`: claiming the whole range at once
 * tells Beam "this range is not further splittable", so at runtime neither can the remaining partitions be handed to
 * idle workers, nor can progress be reported. After claiming one at a time, the slow partition gets automatically
 * offloaded.
 *
 * @author wuya
 */
@DoFn.BoundedPerElement
class MongoReadFn(
    private val connectionUri: String,
    private val codec: MongoRowCodec,
    private val fetchSize: Int,
    private val limit: Long = -1,
) : DoFn<MongoReadSplit, Row>() {

    @Transient
    private var client: MongoClient? = null

    /**
     * Used by unit tests to inject a fake client; null on the production path.
     *
     * This is **not** marked `@Transient`, for the same reason as [MongoWriteFn.testClient]: with the annotation,
     * the injected fake client would vanish on the worker.
     */
    internal var testClient: MongoClient? = null

    @Setup
    fun setup() {
        client = testClient ?: MongoClients.create(connectionUri)
    }

    @Teardown
    fun tearDown() {
        runCatching { client?.close() }
        client = null
    }

    /**
     * The partition count is fixed at graph-construction time, so no DB connection is needed here — before the refactor
     * every computed initial restriction created a new MongoClient (connection pool + background monitor thread) and dropped it.
     */
    @GetInitialRestriction
    fun getInitialRestriction(@Element split: MongoReadSplit): OffsetRange =
        OffsetRange(0, split.partitionFilters.size.toLong())

    @SplitRestriction
    fun splitRestriction(
        @Element split: MongoReadSplit,
        @Restriction restriction: OffsetRange,
        receiver: OutputReceiver<OffsetRange>,
    ) {
        if (restriction.to <= restriction.from) {
            return
        }
        // Partitions are already balanced by $bucketAuto, so one initial split per partition is enough
        restriction.split(1, 1).forEach { receiver.output(it) }
    }

    @ProcessElement
    fun processElement(
        @Element split: MongoReadSplit,
        tracker: RestrictionTracker<OffsetRange, Long>,
        receiver: OutputReceiver<Row>,
    ) {
        val range = tracker.currentRestriction()
        var index = range.from
        while (index < range.to) {
            if (!tracker.tryClaim(index)) {
                return
            }
            readPartition(split, index.toInt(), receiver)
            index++
        }
    }

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = restriction.newTracker()

    @GetRestrictionCoder
    fun restrictionCoder(): Coder<OffsetRange> = OffsetRange.Coder()

    private fun readPartition(split: MongoReadSplit, index: Int, receiver: OutputReceiver<Row>) {
        val filter = MongoBuckets.parse(split.partitionFilters[index])
        val collection = checkNotNull(client) { "MongoClient not initialized" }
            .getDatabase(split.database)
            .getCollection(split.collection, Document::class.java)

        var count = 0L
        var iterable = collection.find(filter)
            // Only fetch the declared fields so MongoDB transfers less data
            .projection(codec.projection())
            // fetch_size is how many rows the cursor fetches per round trip; before the refactor it was mistakenly used as "how many rows per partition"
            .batchSize(fetchSize)
        // LIMIT must be global: partitioned reads would turn limit into "limit per partition", so when a row limit is set
        // the provider falls back to a single partition; here it only applies to a single find, pushing the cap down to MongoDB.
        // The MongoDB driver's limit parameter is Int; when the total limit is larger we must not narrow it overflowing to a
        // negative number, so the Long counter below acts as the safeguard. Common small limits are still pushed to the server to avoid pulling extra data.
        cursorLimit(limit)?.let { iterable = iterable.limit(it) }
        iterable.iterator()
            .use { cursor ->
                while ((limit < 0 || count < limit) && cursor.hasNext()) {
                    receiver.output(codec.toRow(cursor.next()))
                    count++
                }
            }
        RECORDS_READ.inc(count)
        LOGGER.info("{}.{} partition [{}] read {} rows", split.database, split.collection, index, count)
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(MongoReadFn::class.java)
        private val RECORDS_READ = Metrics.counter(MongoReadFn::class.java, "records_read")

        /** The single-cursor limit that can be safely pushed to the Mongo Java driver; larger totals are controlled by the Long counter. */
        internal fun cursorLimit(limit: Long): Int? =
            limit.takeIf { it in 1..Int.MAX_VALUE.toLong() }?.toInt()
    }
}
