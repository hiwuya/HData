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

    /** 已经包装成死信记录的失败行，带着原始行自己的时间戳与窗口。 */
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
        val c = checkNotNull(client) { "ES 客户端未初始化" }
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
                        reject(buffered[i].vs, IllegalStateException("写入 ES 失败: ${err.type()} ${err.reason()}"))
                    }
                }
            } else {
                RECORDS_WRITTEN.inc(buffered.size.toLong())
            }
        } catch (e: Exception) {
            // 连接层面的问题，整批都没写进去
            if (!deadLetter) throw e
            LOGGER.warn("ES 批量写入失败，整批转入死信: {}", e.message)
            buffered.forEach { reject(it.vs, e) }
        } finally {
            buffered.clear()
        }
    }

    /**
     * 死信记录在这里就包装好。
     *
     * 之前是先把原始行攒起来、到 `@FinishBundle` 才用 `row.getString("value")` 当错误信息现编一个
     * `RuntimeException`：错误信息其实是文档内容而不是 ES 的报错，而且配了 `schema_fields`
     * （输入行根本没有 `value` 字段）时这一句直接抛 `IllegalArgumentException`——
     * 恰好在 `error_handling` 本该兜住失败的时候把作业弄挂了。
     */
    private fun reject(record: ValueInSingleWindow<Row>, e: Exception) {
        if (!deadLetter) throw e
        LOGGER.warn("写入 ES 失败，转入死信: {}", e.message)
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
