package me.jayer.hdata.mongodb.transform

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Sorts
import me.jayer.hdata.mongodb.buildSchema
import me.jayer.hdata.mongodb.documentToRow
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.values.Row
import org.bson.Document
import org.slf4j.LoggerFactory
import java.io.Serializable

/**
 * 一个 [MongoReadSplit] 表示要读取的一个集合（元素本身），限制用 `OffsetRange` 表示文档序号区间
 * `[from, to)`，交给 Beam 的 splittable DoFn 按 [fetchSize] 切成多段，每段用 `skip/limit`
 * （按 `_id` 排序）读。
 */
data class MongoReadSplit(
    val connectionUri: String,
    val database: String,
    val collection: String,
) : Serializable

/**
 * 按文档序号区间并行读 MongoDB，是标准的 Splittable DoFn。
 *
 * 连接信息通过构造函数传入（与 [DoFn] 一起序列化下发），`@ProcessElement` 用
 * `tryClaim(range.to - 1)` 一次性认领整段，中途放弃会重读该段（at-least-once）。
 */
@DoFn.BoundedPerElement
class MongoReadFn(
    private val connectionUri: String,
    private val database: String,
    private val collection: String,
    private val schemaFields: List<String>,
    private val fetchSize: Int,
) : DoFn<MongoReadSplit, Row>() {

    @Transient
    private var client: MongoClient? = null

    private val schema: Schema = buildSchema(schemaFields)

    @Setup
    fun setup() {
        client = MongoClients.create(connectionUri)
    }

    @Teardown
    fun tearDown() {
        runCatching { client?.close() }
        client = null
    }

    @GetInitialRestriction
    fun getInitialRestriction(@Element split: MongoReadSplit): OffsetRange =
        withClient(split.connectionUri) { c ->
            val count = collection(c, split).countDocuments()
            LOGGER.info("collection[{}.{}] 共 {} 个文档", split.database, split.collection, count)
            if (count <= 0) OffsetRange(0, 0) else OffsetRange(0, count)
        }

    @SplitRestriction
    fun splitRestriction(
        @Element split: MongoReadSplit,
        @Restriction restriction: OffsetRange,
        receiver: OutputReceiver<OffsetRange>,
    ) {
        val span = restriction.to - restriction.from
        if (span <= 0) {
            return
        }
        val perSplit = fetchSize.toLong().coerceAtLeast(1)
        restriction.split(perSplit, 1).forEach { receiver.output(it) }
        LOGGER.info("collection[{}.{}] 切分为 {} 段", split.database, split.collection, restriction.split(perSplit, 1).size)
    }

    @ProcessElement
    fun processElement(
        @Element split: MongoReadSplit,
        tracker: RestrictionTracker<OffsetRange, Long>,
        receiver: OutputReceiver<Row>,
    ) {
        val range = tracker.currentRestriction()
        if (range.to <= range.from) {
            return
        }
        if (!tracker.tryClaim(range.to - 1)) {
            return
        }
        val c = checkNotNull(client) { "MongoClient 未初始化" }
        val iter = collection(c, split)
            .find()
            .sort(Sorts.ascending("_id"))
            .skip(range.from.toInt())
            .limit((range.to - range.from).toInt())
            .iterator()
        var count = 0L
        while (iter.hasNext()) {
            val doc = iter.next()
            receiver.output(documentToRow(doc, schema, schemaFields))
            count++
        }
        RECORDS_READ.inc(count)
        LOGGER.info("collection[{}.{}] 区间 [{}, {}) 读完 {} 条", split.database, split.collection, range.from, range.to, count)
    }

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = restriction.newTracker()

    @GetRestrictionCoder
    fun restrictionCoder(): Coder<OffsetRange> = OffsetRange.Coder()

    private fun collection(c: MongoClient, split: MongoReadSplit) =
        c.getDatabase(split.database).getCollection(split.collection, Document::class.java)

    private fun <T> withClient(uri: String, block: (MongoClient) -> T): T {
        val c = MongoClients.create(uri)
        try {
            return block(c)
        } finally {
            runCatching { c.close() }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(MongoReadFn::class.java)
        private val RECORDS_READ = Metrics.counter(MongoReadFn::class.java, "records_read")
    }
}
