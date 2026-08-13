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
 * 攒批 `BulkRequest` 写入 ES 6.x，写失败且开了死信时把坏记录转死信流；
 * 没开死信时异常直接抛出，作业失败。
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
) : DoFn<Row, Row>() {

    @Transient
    private var client: RestHighLevelClient? = null

    private val buffered = mutableListOf<ValueInSingleWindow<Row>>()
    private val failures = mutableListOf<ValueInSingleWindow<Row>>()

    @Setup
    fun setup() {
        client = newClient()
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
        val c = checkNotNull(client) { "ES 客户端未初始化" }
        val bulk = BulkRequest()
        buffered.forEach { record -> bulk.add(buildIndexRequest(record.value)) }
        try {
            val resp: BulkResponse = c.bulk(bulk, RequestOptions.DEFAULT)
            if (resp.hasFailures()) {
                if (!deadLetter) {
                    val first = resp.items.firstOrNull { it.isFailed }
                    throw IOException(first?.failureMessage ?: "ES 批量写入失败")
                }
                resp.items.forEachIndexed { i, item ->
                    if (item.isFailed) {
                        val rec = buffered[i]
                        RECORDS_REJECTED.inc()
                        failures.add(
                            ValueInSingleWindow.of(
                                ErrorSchemas.failure(errorSchema, rec.value, IOException(item.failureMessage), transformName),
                                rec.timestamp,
                                rec.window,
                                rec.paneInfo,
                            ),
                        )
                    }
                }
            }
            RECORDS_WRITTEN.inc(buffered.size.toLong())
        } catch (e: Exception) {
            if (!deadLetter) {
                throw e
            }
            LOGGER.warn("ES 批量写入失败，转入死信: {}", e.message)
            buffered.forEach { rec ->
                RECORDS_REJECTED.inc()
                failures.add(
                    ValueInSingleWindow.of(
                        ErrorSchemas.failure(errorSchema, rec.value, e, transformName),
                        rec.timestamp,
                        rec.window,
                        rec.paneInfo,
                    ),
                )
            }
        }
        buffered.clear()
    }

    private fun buildIndexRequest(row: Row): IndexRequest {
        val idx = if (index.isBlank() && inputSchema.hasField("index")) row.getString("index") else index
        return if (fields.isEmpty()) {
            val json = row.getString("value")
                ?: throw IllegalStateException("写 ES 的行缺少 value 字段（未配置 schema_fields）")
            IndexRequest(idx).source(json, XContentType.JSON)
        } else {
            val source = LinkedHashMap<String, Any?>()
            fields.forEach { f -> source[f.name] = esValue(f.type, row.getValue(f.name)) }
            IndexRequest(idx).source(source as Map<String, Any>)
        }
    }

    private fun newClient(): RestHighLevelClient {
        val hosts = nodes.map { org.apache.http.HttpHost.create(it) }.toTypedArray()
        val builder = RestClient.builder(*hosts)
        if (username.isNotBlank() && password.isNotBlank()) {
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
