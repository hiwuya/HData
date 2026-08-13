package me.jayer.hdata.mongodb.transform

import com.mongodb.MongoBulkWriteException
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.model.BulkWriteOptions
import com.mongodb.client.model.Filters
import com.mongodb.client.model.InsertOneModel
import com.mongodb.client.model.ReplaceOneModel
import com.mongodb.client.model.ReplaceOptions
import com.mongodb.client.model.WriteModel
import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.mongodb.MongoRowCodec
import me.jayer.hdata.mongodb.MongoWriteConfig
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.bson.Document
import org.slf4j.LoggerFactory

/**
 * 攒批写入 MongoDB，支持死信输出。
 *
 * 提交走一次 `bulkWrite`，而不是重构前的**逐条 `insertOne`**——那样写 `batch_size` 只是攒在内存里，
 * 真正发出去还是一条一个往返，等于把批量写的意义抹掉了。
 *
 * `bulkWrite` 用 `ordered=false`：一条失败不会让后面的都不执行，
 * 失败信息在 `MongoBulkWriteException.writeErrors` 里按下标给出，可以精确到行。
 *
 * @author wuya
 */
class MongoWriteFn(
    private val config: MongoWriteConfig,
    private val codec: MongoRowCodec,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
) : DoFn<Row, Row>() {

    @Transient
    private var client: MongoClient? = null

    @Transient
    private var buffered: MutableList<Pending>? = null

    @Transient
    private var failures: MutableList<ValueInSingleWindow<Row>>? = null

    private class Pending(val record: ValueInSingleWindow<Row>, val model: WriteModel<Document>)

    @Setup
    fun setup() {
        client = MongoClients.create(config.connectionUri)
        buffered = mutableListOf()
        failures = mutableListOf()
    }

    @Teardown
    fun tearDown() {
        runCatching { client?.close() }
        client = null
    }

    @StartBundle
    fun startBundle() {
        buffered?.clear()
        failures?.clear()
    }

    @ProcessElement
    fun processElement(
        @Element row: Row,
        @Timestamp timestamp: org.joda.time.Instant,
        window: BoundedWindow,
        pane: PaneInfo,
    ) {
        val record = ValueInSingleWindow.of(row, timestamp, window, pane)
        val model = try {
            toModel(row)
        } catch (e: Exception) {
            reject(record, e)
            return
        }
        val queue = checkNotNull(buffered) { "写入器未初始化" }
        queue.add(Pending(record, model))
        if (queue.size >= config.batchSize) {
            flush()
        }
    }

    @FinishBundle
    fun finishBundle(context: FinishBundleContext) {
        flush()
        val rejected = checkNotNull(failures)
        rejected.forEach { context.output(it.value, it.timestamp, it.window) }
        rejected.clear()
    }

    private fun flush() {
        val queue = checkNotNull(buffered)
        if (queue.isEmpty()) {
            return
        }
        val collection = checkNotNull(client) { "MongoClient 未初始化" }
            .getDatabase(config.database)
            .getCollection(config.collection, Document::class.java)
        try {
            collection.bulkWrite(queue.map { it.model }, BulkWriteOptions().ordered(false))
            RECORDS_WRITTEN.inc(queue.size.toLong())
        } catch (e: MongoBulkWriteException) {
            // 只有 writeErrors 里点名的那几条失败了，其余已经写进去
            val failedIndexes = e.writeErrors.associateBy { it.index }
            queue.forEachIndexed { index, pending ->
                val error = failedIndexes[index]
                if (error == null) {
                    RECORDS_WRITTEN.inc()
                } else {
                    reject(pending.record, IllegalStateException("MongoDB 写入失败(${error.code}): ${error.message}"))
                }
            }
        } catch (e: Exception) {
            // 连接层面的问题，整批都没写进去
            queue.forEach { reject(it.record, e) }
        } finally {
            queue.clear()
        }
    }

    /**
     * 配了 [MongoWriteConfig.upsertKeys] 就按主键覆盖写，否则纯插入。
     *
     * 没有 upsert 时重跑作业会造出重复文档——这不是 bug，但值得在文档里说清楚，
     * 所以这里把选择权交给配置而不是写死成 insert。
     */
    private fun toModel(row: Row): WriteModel<Document> {
        val doc = codec.toDocument(row)
        if (!config.upsert) {
            return InsertOneModel(doc)
        }
        val filter = Filters.and(
            config.upsertKeys.map { key ->
                require(doc.containsKey(key)) { "upsert_keys 声明的字段[$key] 在待写文档里不存在" }
                Filters.eq(key, doc[key])
            }
        )
        return ReplaceOneModel(filter, doc, ReplaceOptions().upsert(true))
    }

    private fun reject(record: ValueInSingleWindow<Row>, e: Exception) {
        if (!deadLetter) {
            throw e
        }
        LOGGER.warn("写入 MongoDB 失败，转入死信: {}", e.message)
        RECORDS_REJECTED.inc()
        checkNotNull(failures).add(
            ValueInSingleWindow.of(
                ErrorSchemas.failure(errorSchema, record.value, e, transformName),
                record.timestamp,
                record.window,
                record.paneInfo,
            )
        )
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(MongoWriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(MongoWriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(MongoWriteFn::class.java, "records_rejected")
    }
}
