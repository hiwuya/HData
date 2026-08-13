package me.jayer.hdata.mongodb.transform

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.mongodb.rowToDocument
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.bson.Document
import org.slf4j.LoggerFactory
import java.io.Serializable

/**
 * 攒批写入 MongoDB，批满 [batchSize] 时 flush。写失败且开了死信时逐条重试，真正写不进去的记录进死信流；
 * 没开死信时异常直接抛出，作业失败。
 */
class MongoWriteFn(
    private val connectionUri: String,
    private val database: String,
    private val collection: String,
    private val schemaFields: List<String>,
    private val batchSize: Int,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
) : DoFn<Row, Row>() {

    @Transient
    private var client: MongoClient? = null

    private val buffered = mutableListOf<ValueInSingleWindow<Row>>()
    private val failures = mutableListOf<ValueInSingleWindow<Row>>()

    @Setup
    fun setup() {
        client = MongoClients.create(connectionUri)
    }

    @StartBundle
    fun startBundle() {
        buffered.clear()
        failures.clear()
    }

    @ProcessElement
    fun processElement(
        @Element row: Row,
        @Timestamp timestamp: org.joda.time.Instant,
        window: BoundedWindow,
        pane: PaneInfo,
    ) {
        buffered.add(ValueInSingleWindow.of(row, timestamp, window, pane))
        if (buffered.size >= batchSize) {
            flush()
        }
    }

    @FinishBundle
    fun finishBundle(context: FinishBundleContext) {
        flush()
        failures.forEach { context.output(it.value, it.timestamp, it.window) }
        failures.clear()
    }

    @Teardown
    fun tearDown() {
        runCatching { client?.close() }
        client = null
    }

    private fun flush() {
        if (buffered.isEmpty()) {
            return
        }
        val c = checkNotNull(client) { "MongoClient 未初始化" }
        val coll = c.getDatabase(database).getCollection(collection, Document::class.java)
        buffered.forEach { record ->
            try {
                coll.insertOne(rowToDocument(record.value, schemaFields))
                RECORDS_WRITTEN.inc()
            } catch (e: Exception) {
                if (!deadLetter) {
                    throw e
                }
                LOGGER.warn("写入 MongoDB 失败，转入死信: {}", e.message)
                RECORDS_REJECTED.inc()
                failures.add(
                    ValueInSingleWindow.of(
                        ErrorSchemas.failure(errorSchema, record.value, e, transformName),
                        record.timestamp,
                        record.window,
                        record.paneInfo,
                    )
                )
            }
        }
        buffered.clear()
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(MongoWriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(MongoWriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(MongoWriteFn::class.java, "records_rejected")
    }
}
