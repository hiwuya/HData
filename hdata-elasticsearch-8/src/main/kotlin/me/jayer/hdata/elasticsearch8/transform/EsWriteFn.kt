package me.jayer.hdata.elasticsearch8.transform

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch._types.ErrorCause
import co.elastic.clients.elasticsearch.core.BulkRequest
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation
import tools.jackson.databind.json.JsonMapper
import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.elasticsearch8.DOCUMENT_FIELD
import me.jayer.hdata.elasticsearch8.EsClientFactory
import me.jayer.hdata.elasticsearch8.EsWriteConfig
import me.jayer.hdata.elasticsearch8.buildEsClient
import me.jayer.hdata.elasticsearch8.parseSchemaFields
import me.jayer.hdata.elasticsearch8.toJsonValue
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.elasticsearch.client.RestClient
import org.slf4j.LoggerFactory
import java.util.LinkedHashMap

/**
 * Buffers bulk writes to Elasticsearch 8.x and sends failed records to dead letter when enabled.
 *
 * @author wuya
 */
class EsWriteFn(
    private val config: EsWriteConfig,
    private val inputSchema: Schema,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
) : DoFn<Row, Row>() {

    @Transient
    private var client: ElasticsearchClient? = null

    /**
     * Serializable client-factory injection point for tests; production uses null and creates the client on workers.
     */
    internal var clientFactory: EsClientFactory? = null

    @Transient
    private var restClient: RestClient? = null

    /**
     * Jackson 3 mapper, constructed on the worker rather than carried in serialized DoFn state.
     */
    @Transient
    private var jsonMapper: JsonMapper? = null

    private val schemaFields = parseSchemaFields(config.schemaFields)

    private data class Buffered(val vs: ValueInSingleWindow<Row>, val doc: Map<String, Any?>)

    private val buffered = mutableListOf<Buffered>()

    /** Dead-letter records preserving each failed row's original timestamp and window. */
    private val failures = mutableListOf<ValueInSingleWindow<Row>>()

    @Setup
    fun setup() {
        val factory = clientFactory
        if (factory != null) {
            client = factory.create()
            jsonMapper = JsonMapper.builder().build()
            return
        }
        val (c, rc) = buildEsClient(config.connectionUri, config.apiKey, config.username, config.password)
        client = c
        restClient = rc
        jsonMapper = JsonMapper.builder().build()
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
        pane: org.apache.beam.sdk.transforms.windowing.PaneInfo,
    ) {
        val record = ValueInSingleWindow.of(row, timestamp, window, pane)
        val document = try {
            toDocument(row)
        } catch (e: Exception) {
            reject(record, e)
            return
        }
        buffered.add(Buffered(record, document))
        if (buffered.size >= config.batchSize) {
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
        runCatching { restClient?.close() }
        restClient = null
        client = null
    }

    private fun flush() {
        if (buffered.isEmpty()) return
        val c = checkNotNull(client) { "Elasticsearch client is not initialized" }
        val ops = buffered.map { (_, doc) ->
            BulkOperation.of { b -> b.index(IndexOperation.of { i -> i.index(config.index).document(doc) }) }
        }
        try {
            val resp = c.bulk(BulkRequest.of { it.operations(ops) })
            if (resp.errors()) {
                resp.items().forEachIndexed { i, item: BulkResponseItem ->
                    val err: ErrorCause? = item.error()
                    if (err == null) {
                        RECORDS_WRITTEN.inc()
                    } else {
                        reject(buffered[i].vs, IllegalStateException("Elasticsearch write failed: ${err.type()} ${err.reason()}"))
                    }
                }
            } else {
                RECORDS_WRITTEN.inc(buffered.size.toLong())
            }
        } catch (e: Exception) {
            // A connection failure means the complete batch was not written.
            if (!deadLetter) throw e
            LOGGER.warn("Elasticsearch bulk write failed; sending the batch to dead letter: {}", e.message)
            buffered.forEach { reject(it.vs, e) }
        } finally {
            buffered.clear()
        }
    }

    /**
     * Wraps the dead-letter record immediately, retaining the real exception rather than fabricating one from row data.
     */
    private fun reject(record: ValueInSingleWindow<Row>, e: Exception) {
        if (!deadLetter) throw e
        LOGGER.warn("Elasticsearch write failed; sending record to dead letter: {}", e.message)
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

    private fun toDocument(row: Row): Map<String, Any?> {
        if (schemaFields.isEmpty()) {
            // Keep this field aligned with ReadFromElasticsearch8 so document-mode rows can round-trip.
            val json = row.getString(DOCUMENT_FIELD)
                ?: throw IllegalStateException("row written to Elasticsearch is missing $DOCUMENT_FIELD (schema_fields is not configured)")
            return checkNotNull(jsonMapper).readValue(json, LinkedHashMap::class.java) as Map<String, Any?>
        }
        val map = LinkedHashMap<String, Any?>()
        schemaFields.forEach { (name, type) ->
            map[name] = toJsonValue(row.getValue(name), type)
        }
        return map
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(EsWriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(EsWriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(EsWriteFn::class.java, "records_rejected")
    }
}
