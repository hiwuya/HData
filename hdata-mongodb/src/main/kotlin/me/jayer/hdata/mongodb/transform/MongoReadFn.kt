package me.jayer.hdata.mongodb.transform

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import me.jayer.hdata.mongodb.MongoAggregateSpec
import me.jayer.hdata.mongodb.MongoRowCodec
import me.jayer.hdata.mongodb.aggregateSchema
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
 * 一次读取任务：一个集合，外加它被 [MongoBuckets] 切好的若干分片过滤条件。
 *
 * 分片边界在构图阶段就定死并随元素下发，这样每个 worker 看到的边界完全一致。
 */
data class MongoReadSplit(
    val database: String,
    val collection: String,
    /** 每个分片一条完整的过滤条件（扩展 JSON），互不重叠。 */
    val partitionFilters: List<String>,
) : Serializable {
    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * 按 `_id` 区间并行读 MongoDB 的 Splittable DoFn。
 *
 * 限制是分片下标区间 `[from, to)`，`@ProcessElement` **逐个分片认领**：
 *
 * ```kotlin
 * while (index < end) {
 *     if (!tracker.tryClaim(index)) return
 *     readPartition(index)
 * }
 * ```
 *
 * 这一点和重构前的 `tryClaim(range.to - 1)` 有本质区别：一次性认领整段等于告诉 Beam
 * "这段不可再分"，运行时既没法把剩下的分片切给空闲 worker，也拿不到进度。
 * 逐个认领之后，慢的那一份会被自动分担出去。
 *
 * @author wuya
 */
@DoFn.BoundedPerElement
class MongoReadFn(
    private val connectionUri: String,
    private val codec: MongoRowCodec,
    private val fetchSize: Int,
    private val limit: Long = -1,
    /** 非空表示聚合下推模式：用 `aggregate` 管道替代 `find`，输出聚合结果行。 */
    private val aggregateSpecs: List<MongoAggregateSpec> = emptyList(),
) : DoFn<MongoReadSplit, Row>() {

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

    /**
     * 分片数在构图阶段就确定了，所以这里不需要连库——重构前每算一次初始限制都要
     * 新建一个 MongoClient（连接池 + 后台监控线程）再扔掉。
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
        // 分片已经按 $bucketAuto 均衡过了，一个分片一份初始切分即可
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
        if (aggregateSpecs.isNotEmpty()) {
            readAggregate(split, index, receiver)
            return
        }
        val filter = MongoBuckets.parse(split.partitionFilters[index])
        val collection = checkNotNull(client) { "MongoClient 未初始化" }
            .getDatabase(split.database)
            .getCollection(split.collection, Document::class.java)

        var count = 0L
        var iterable = collection.find(filter)
            // 只取声明过的字段，让 MongoDB 少传一些数据
            .projection(codec.projection())
            // fetch_size 是游标每次往返取多少条，重构前它被当成"每个分片读多少条"用了
            .batchSize(fetchSize)
        // LIMIT 必须是全局的：分片读会把 limit 变成"每片 limit"，所以限行数时由 provider 退化为单分片；
        // 这里只在单条 find 上生效，把上限下推给 MongoDB。
        if (limit > 0) iterable = iterable.limit(limit.toInt())
        iterable.iterator()
            .use { cursor ->
                while (cursor.hasNext()) {
                    receiver.output(codec.toRow(cursor.next()))
                    count++
                }
            }
        RECORDS_READ.inc(count)
        LOGGER.info("{}.{} 分片[{}] 读出 {} 条", split.database, split.collection, index, count)
    }

    /**
     * 聚合下推：用 `[$match filter, $group {...}]` 管道替代 `find`。
     * 强制单分片（provider 已保证 split 只有一条），所以 `$group` 的 `_id` 为 null 得到的是整表聚合结果。
     * 空集合时 `$group` 不产生任何文档，这里补一行 `count=0`、其余为 null，让 `SELECT COUNT(*) FROM t` 这类查询
     * 在空表上也返回 0 而不是空结果。
     */
    private fun readAggregate(split: MongoReadSplit, index: Int, receiver: OutputReceiver<Row>) {
        val filter = MongoBuckets.parse(split.partitionFilters[index])
        val collection = checkNotNull(client) { "MongoClient 未初始化" }
            .getDatabase(split.database)
            .getCollection(split.collection, Document::class.java)

        val pipeline = buildAggregatePipeline(filter, aggregateSpecs)
        val iter = collection.aggregate(pipeline, Document::class.java)
            .allowDiskUse(true)
            .iterator()
        if (!iter.hasNext()) {
            receiver.output(emptyAggregateRow())
        } else {
            receiver.output(toAggregateRow(iter.next()))
        }
        LOGGER.info("{}.{} 聚合下推产出 1 行", split.database, split.collection)
    }

    private fun toAggregateRow(doc: Document): Row {
        val schema = aggregateSchema(aggregateSpecs)
        val builder = Row.withSchema(schema)
        aggregateSpecs.forEach { spec ->
            val raw = doc[spec.alias]
            when (spec.type) {
                "count" -> builder.addValue((raw as? Number)?.toLong() ?: 0L)
                else -> builder.addValue((raw as? Number)?.toDouble())
            }
        }
        return builder.build()
    }

    private fun emptyAggregateRow(): Row {
        val schema = aggregateSchema(aggregateSpecs)
        val builder = Row.withSchema(schema)
        aggregateSpecs.forEach { spec ->
            if (spec.type == "count") builder.addValue(0L) else builder.addValue(null)
        }
        return builder.build()
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(MongoReadFn::class.java)
        private val RECORDS_READ = Metrics.counter(MongoReadFn::class.java, "records_read")
    }
}

/**
 * 把聚合规格翻译成 MongoDB 聚合管道 `[$match filter, $group {...}]`。
 * 抽成独立函数便于单测——聚合下推到底有没有生效、下的什么，全看这条管道。
 */
internal fun buildAggregatePipeline(filter: org.bson.BsonDocument, specs: List<MongoAggregateSpec>): List<Document> {
    val group = Document("_id", null).apply {
        specs.forEach { spec ->
            val expr = if (spec.type == "count") {
                if (spec.column.isBlank() || spec.column == "*") Document("\$sum", 1) else Document("\$sum", "\$${spec.column}")
            } else {
                Document("\$${spec.type}", "\$${spec.column}")
            }
            append(spec.alias, expr)
        }
    }
    return listOf(Document("\$match", filter), Document("\$group", group))
}
