package me.jayer.hdata.elasticsearch6

import me.jayer.hdata.core.error.ErrorSchemas
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.xcontent.XContentType
import org.joda.time.Instant
import org.slf4j.LoggerFactory
import java.io.IOException

/**
 * Batches `BulkRequest`s to write to ES 6.x; on write failure with the dead letter enabled it sends the bad records to
 * the dead-letter stream; without the dead letter the exception is thrown directly and the job fails.
 *
 * @author wuya
 */
class Elasticsearch6WriteFn(
    private val nodes: List<String>,
    private val index: String,
    private val username: String,
    private val password: String,
    private val fields: List<EsField>,
    private val batchSize: Int,
    private val inputSchema: Schema,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
    /** ES 6.x still requires a mapping type per document; unset it and the server rejects every write. */
    private val docType: String = "_doc",
) : DoFn<Row, Row>() {

    @Transient
    private var client: RestHighLevelClient? = null

    /** The client factory used for test injection; null on the production path, see [Es6ClientFactory] for the reason. */
    internal var clientFactory: Es6ClientFactory? = null

    private data class Buffered(val record: ValueInSingleWindow<Row>, val request: IndexRequest)

    private val buffered = mutableListOf<Buffered>()
    private val failures = mutableListOf<ValueInSingleWindow<Row>>()

    @Setup
    fun setup() {
        client = clientFactory?.create() ?: newClient()
    }

    @StartBundle
    fun startBundle() {
        buffered.clear()
        failures.clear()
    }

    @ProcessElement
    fun processElement(
        @Element row: Row,
        @Timestamp timestamp: Instant,
        window: BoundedWindow,
        pane: PaneInfo,
    ) {
        val record = ValueInSingleWindow.of(row, timestamp, window, pane)
        val request = try {
            buildIndexRequest(row)
        } catch (e: Exception) {
            reject(record, e)
            return
        }
        buffered.add(Buffered(record, request))
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
        val c = checkNotNull(client) { "ES client is not initialized" }
        val bulk = BulkRequest()
        buffered.forEach { bulk.add(it.request) }
        try {
            val resp: BulkResponse = c.bulk(bulk, RequestOptions.DEFAULT)
            if (!resp.hasFailures()) {
                RECORDS_WRITTEN.inc(buffered.size.toLong())
            } else {
                if (!deadLetter) {
                    val first = resp.items.firstOrNull { it.isFailed }
                    throw IOException(first?.failureMessage ?: "ES bulk write failed")
                }
                // Count only the truly failed records as rejected, and the rest as written —
                // previously the whole batch was counted as written once regardless of success/failure, so failed rows
                // were counted into both metrics at the same time.
                resp.items.forEachIndexed { i, item ->
                    if (!item.isFailed) {
                        RECORDS_WRITTEN.inc()
                        return@forEachIndexed
                    }
                    reject(buffered[i].record, IOException(item.failureMessage))
                }
            }
        } catch (e: Exception) {
            if (!deadLetter) {
                throw e
            }
            LOGGER.warn("ES bulk write failed, sending to the dead letter: {}", e.message)
            buffered.forEach { reject(it.record, e) }
        } finally {
            buffered.clear()
        }
    }

    private fun reject(record: ValueInSingleWindow<Row>, error: Exception) {
        if (!deadLetter) throw error
        RECORDS_REJECTED.inc()
        failures.add(
            ValueInSingleWindow.of(
                ErrorSchemas.failure(errorSchema, record.value, error, transformName),
                record.timestamp,
                record.window,
                record.paneInfo,
            ),
        )
    }

    private fun buildIndexRequest(row: Row): IndexRequest {
        val idx = if (index.isBlank() && inputSchema.hasField("index")) row.getString("index") else index
        return if (fields.isEmpty()) {
            // The column name must match what `ReadFromElasticsearch6` produces, otherwise not a single row read out
            // can be written back: the read side producing `document` while the write side looks for `value` is the
            // archetype of this class of bug.
            val json = row.getString(DOCUMENT_FIELD)
                ?: throw IllegalStateException("the row being written to ES is missing the $DOCUMENT_FIELD field (schema_fields not configured)")
            IndexRequest(idx, docType).source(json, XContentType.JSON)
        } else {
            val source = LinkedHashMap<String, Any?>()
            fields.forEach { f -> source[f.name] = esValue(f.type, row.getValue(f.name)) }
            IndexRequest(idx, docType).source(source as Map<String, Any>)
        }
    }

    private fun newClient(): RestHighLevelClient {
        val hosts = parseElasticsearch6Hosts(nodes)
        val builder = RestClient.builder(*hosts)
        if (username.isNotBlank()) {
            val creds = org.apache.http.impl.client.BasicCredentialsProvider()
            creds.setCredentials(
                org.apache.http.auth.AuthScope.ANY,
                org.apache.http.auth.UsernamePasswordCredentials(username, password),
            )
            builder.setHttpClientConfigCallback { it.setDefaultCredentialsProvider(creds) }
        }
        return RestHighLevelClient(builder)
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(Elasticsearch6WriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(Elasticsearch6WriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(Elasticsearch6WriteFn::class.java, "records_rejected")
    }
}
