package me.jayer.hdata.elasticsearch8.transform

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch._types.ErrorCause
import co.elastic.clients.elasticsearch.core.BulkRequest
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation
import com.fasterxml.jackson.databind.ObjectMapper
import me.jayer.hdata.core.error.ErrorSchemas
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
 * 攒批用 bulk API 写入 Elasticsearch 8.x，写失败且开了死信时退回死信流，否则直接抛异常。
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

    @Transient
    private var restClient: RestClient? = null

    private val jsonMapper = ObjectMapper()
    private val schemaFields = parseSchemaFields(config.schemaFields)

    private data class Buffered(val vs: ValueInSingleWindow<Row>, val doc: Map<String, Any?>)

    private val buffered = mutableListOf<Buffered>()
    private val failures = mutableListOf<ValueInSingleWindow<Row>>()

    @Setup
    fun setup() {
        val (c, rc) = buildEsClient(config.connectionUri, config.apiKey, config.username, config.password)
        client = c
        restClient = rc
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
        buffered.add(Buffered(ValueInSingleWindow.of(row, timestamp, window, pane), toDocument(row)))
        if (buffered.size >= config.batchSize) {
            flush()
        }
    }

    @FinishBundle
    fun finishBundle(context: FinishBundleContext) {
        flush()
        failures.forEach { vs ->
            context.output(
                ErrorSchemas.failure(errorSchema, vs.value, RuntimeException(vs.value.getString("value")), transformName),
                vs.timestamp,
                vs.window,
            )
        }
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
        val c = checkNotNull(client) { "ES 客户端未初始化" }
        val ops = buffered.map { (vs, doc) ->
            BulkOperation.of { b -> b.index(IndexOperation.of { i -> i.index(config.index).document(doc) }) }
        }
        val resp = c.bulk(BulkRequest.of { it.operations(ops) })
        if (resp.errors()) {
            resp.items().forEachIndexed { i, item: BulkResponseItem ->
                val err: ErrorCause? = item.error()
                if (err != null) {
                    if (!deadLetter) {
                        throw IllegalStateException("写入 ES 失败: ${err.type()} ${err.reason()}")
                    }
                    LOGGER.warn("写入 ES 失败，转入死信: {} {}", err.type(), err.reason())
                    RECORDS_REJECTED.inc()
                    failures.add(buffered[i].vs)
                } else {
                    RECORDS_WRITTEN.inc()
                }
            }
        } else {
            RECORDS_WRITTEN.inc(buffered.size.toLong())
        }
        buffered.clear()
    }

    private fun toDocument(row: Row): Map<String, Any?> {
        if (schemaFields.isEmpty()) {
            val json = row.getString("value")
                ?: throw IllegalStateException("写入 ES 的行缺少 value 字段")
            return jsonMapper.readValue(json, LinkedHashMap::class.java) as Map<String, Any?>
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
