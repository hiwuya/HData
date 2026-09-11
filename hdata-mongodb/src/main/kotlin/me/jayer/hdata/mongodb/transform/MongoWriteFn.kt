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
import com.mongodb.bulk.BulkWriteError
import com.mongodb.bulk.WriteConcernError
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
 * Buffered bulk write to MongoDB, with dead-letter output support.
 *
 * The commit goes through a single `bulkWrite`, rather than the pre-refactor **per-row `insertOne`** — with that
 * approach `batch_size` only buffered in memory, and what was actually sent was still one round trip per row,
 * which defeats the whole point of bulk writing.
 *
 * `bulkWrite` uses `ordered=false`: one failure does not prevent the rest from executing, and the failure info is
 * given by index in `MongoBulkWriteException.writeErrors`, so it can be pinpointed to the exact row.
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

    /**
     * Used by unit tests to inject a fake client; null on the production path.
     *
     * This is **not** marked `@Transient`: the DirectRunner serializes the DoFn, ships it, and deserializes it
     * on the worker, so if annotated the injected fake client would vanish on the worker and the end-to-end test
     * could not inject it. On the production path it is always null, so serializing a null has no cost.
     */
    internal var testClient: MongoClient? = null

    @Transient
    private var buffered: MutableList<Pending>? = null

    @Transient
    private var failures: MutableList<ValueInSingleWindow<Row>>? = null

    private class Pending(val record: ValueInSingleWindow<Row>, val model: WriteModel<Document>)

    @Setup
    fun setup() {
        client = testClient ?: MongoClients.create(config.connectionUri)
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
        val queue = checkNotNull(buffered) { "Writer not initialized" }
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
        val collection = checkNotNull(client) { "MongoClient not initialized" }
            .getDatabase(config.database)
            .getCollection(config.collection, Document::class.java)
        try {
            collection.bulkWrite(queue.map { it.model }, BulkWriteOptions().ordered(false))
            RECORDS_WRITTEN.inc(queue.size.toLong())
        } catch (e: MongoBulkWriteException) {
            queue.forEachIndexed { index, pending ->
                val error = mongoBulkFailureAt(e.writeErrors, e.writeConcernError, index)
                if (error == null) {
                    RECORDS_WRITTEN.inc()
                } else {
                    reject(pending.record, error)
                }
            }
        } catch (e: Exception) {
            // Connection-level problem, the whole batch failed to write
            queue.forEach { reject(it.record, e) }
        } finally {
            queue.clear()
        }
    }

    /**
     * With [MongoWriteConfig.upsertKeys] configured, overwrite by primary key; otherwise pure insert.
     *
     * Without upsert, re-running the job produces duplicate documents — this is not a bug, but worth stating
     * clearly in the docs, so here the choice is left to configuration rather than hard-coded as insert.
     */
    private fun toModel(row: Row): WriteModel<Document> {
        val doc = codec.toDocument(row)
        if (!config.upsert) {
            return InsertOneModel(doc)
        }
        val filter = Filters.and(
            config.upsertKeys.map { key ->
                require(doc.containsKey(key)) { "Field [$key] declared in upsert_keys does not exist in the document to be written" }
                Filters.eq(key, doc[key])
            }
        )
        return ReplaceOneModel(filter, doc, ReplaceOptions().upsert(true))
    }

    private fun reject(record: ValueInSingleWindow<Row>, e: Exception) {
        if (!deadLetter) {
            throw e
        }
        LOGGER.warn("Failed to write to MongoDB, routed to dead letter: {}", e.message)
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

/**
 * Maps a bulk failure precisely to a single row.
 *
 * A normal [BulkWriteError] only affects the index it names; a [WriteConcernError] means the whole batch's
 * acknowledgement/durability result is unreliable, so even if the server may have executed some writes, rows
 * not present in `writeErrors` must never be counted as successful.
 */
internal fun mongoBulkFailureAt(
    writeErrors: List<BulkWriteError>,
    writeConcernError: WriteConcernError?,
    index: Int,
): Exception? {
    if (writeConcernError != null) {
        return IllegalStateException(
            "MongoDB write concern failed (${writeConcernError.code}): ${writeConcernError.message}; the whole batch write result cannot be confirmed"
        )
    }
    val error = writeErrors.firstOrNull { it.index == index } ?: return null
    return IllegalStateException("MongoDB write failed (${error.code}): ${error.message}")
}
