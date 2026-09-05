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
 * 聚合下推的局部聚合阶段：每个 `_id` 分片跑一次 MongoDB 局部 `$group`，产出 [me.jayer.hdata.mongodb.PartialAgg]。
 * 与 [MongoReadFn] 一样是 Splittable DoFn——限制是分片下标区间，逐个分片认领，所以局部聚合天然并行。
 *
 * 分片边界在构图阶段随 [MongoReadSplit] 下发，每个 worker 看到的边界完全一致。
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

    /** 单测注入假 client 用；不参与序列化（@Transient），生产路径为 null。 */
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
        // 分片已经按 $bucketAuto 均衡过了，一个分片一份初始切分即可
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
        val collection = checkNotNull(client) { "MongoClient 未初始化" }
            .getDatabase(split.database)
            .getCollection(split.collection, org.bson.Document::class.java)

        val pipeline = buildPartialAggregatePipeline(filter, specs)
        val iter = collection.aggregate(pipeline, org.bson.Document::class.java)
            .allowDiskUse(true)
            .iterator()
        val doc = iter.use { if (it.hasNext()) it.next() else null }
        receiver.output(partialAggFromDoc(doc, specs))
        LOGGER.info("{}.{} 分片[{}] 局部聚合产出 1 个 PartialAgg", split.database, split.collection, index)
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(MongoPartialAggregateFn::class.java)
    }
}
