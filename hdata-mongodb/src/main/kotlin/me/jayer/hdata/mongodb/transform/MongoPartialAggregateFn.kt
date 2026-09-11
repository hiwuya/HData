package me.jayer.hdata.mongodb.transform

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import me.jayer.hdata.mongodb.MongoAggregateSpec
import me.jayer.hdata.mongodb.buildPartialAggregatePipeline
import me.jayer.hdata.mongodb.transform.MongoReadSplit
import me.jayer.hdata.mongodb.internal.MongoBuckets
import me.jayer.hdata.mongodb.partialAggFromDoc
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.slf4j.LoggerFactory
import java.io.Serializable

/**
 * The partial-aggregation stage of push-down aggregation: each `_id` partition runs a local MongoDB `$group`,
 * producing [me.jayer.hdata.mongodb.PartialAgg]. Like [MongoReadFn] it is a Splittable DoFn — the restriction is
 * the partition index range, claimed one partition at a time, so the local aggregation is naturally parallel.
 *
 * Partition boundaries are shipped with [MongoReadSplit] at graph-construction time, so every worker sees exactly
 * the same boundaries.
 *
 * @author wuya
 */
@DoFn.BoundedPerElement
class MongoPartialAggregateFn(
    private val connectionUri: String,
    private val specs: List<MongoAggregateSpec>,
) : DoFn<MongoReadSplit, me.jayer.hdata.mongodb.PartialAgg>() {

    @Transient
    private var client: MongoClient? = null

    /** Used by unit tests to inject a fake client; not serialized (@Transient), null on the production path. */
    @Transient
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
        receiver: OutputReceiver<me.jayer.hdata.mongodb.PartialAgg>,
    ) {
        val range = tracker.currentRestriction()
        var index = range.from
        while (index < range.to) {
            if (!tracker.tryClaim(index)) {
                return
            }
            readShard(split, index.toInt(), receiver)
            index++
        }
    }

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = restriction.newTracker()

    @GetRestrictionCoder
    fun restrictionCoder(): Coder<OffsetRange> = OffsetRange.Coder()

    private fun readShard(split: MongoReadSplit, index: Int, receiver: OutputReceiver<me.jayer.hdata.mongodb.PartialAgg>) {
        val filter = MongoBuckets.parse(split.partitionFilters[index])
        val collection = checkNotNull(client) { "MongoClient not initialized" }
            .getDatabase(split.database)
            .getCollection(split.collection, org.bson.Document::class.java)

        val pipeline = buildPartialAggregatePipeline(filter, specs)
        val iter = collection.aggregate(pipeline, org.bson.Document::class.java)
            .allowDiskUse(true)
            .iterator()
        val doc = iter.use { if (it.hasNext()) it.next() else null }
        receiver.output(partialAggFromDoc(doc, specs))
        LOGGER.info("{}.{} partition [{}] partial aggregation produced 1 PartialAgg", split.database, split.collection, index)
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(MongoPartialAggregateFn::class.java)
    }
}
