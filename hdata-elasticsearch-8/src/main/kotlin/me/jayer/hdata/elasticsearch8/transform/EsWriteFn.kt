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

    /**
     * 测试注入用的客户端工厂；生产路径为 null。
     *
     * 不能直接注入客户端实例：客户端既不能用 `Proxy` 伪造，继承出的子类又没法被 Java 序列化，
     * 而 DirectRunner 下发 DoFn 时一定会序列化一遍。工厂可序列化，反序列化后在 worker 里再造假的。
     */
    internal var clientFactory: EsClientFactory? = null

    @Transient
    private var restClient: RestClient? = null

    /**
     * JSON 解析用 Jackson 3（和项目其余部分一致），**不放进序列化状态**：
     * mapper 又大又没必要跟着 DoFn 下发，到 worker 上重建一次即可（读端 [EsReadFn] 也是这么做的）。
     */
    @Transient
    private var jsonMapper: JsonMapper? = null

    private val schemaFields = parseSchemaFields(config.schemaFields)

    private data class Buffered(val vs: ValueInSingleWindow<Row>, val doc: Map<String, Any?>)

    private val buffered = mutableListOf<Buffered>()

    /** 已经包装成死信记录的失败行，带着原始行自己的时间戳与窗口。 */
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
            // 连接层面的问题，整批都没写进去
            if (!deadLetter) throw e
            LOGGER.warn("Elasticsearch bulk write failed; sending the batch to dead letter: {}", e.message)
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
            // 列名必须与 `ReadFromElasticsearch8` 的产出一致，否则读出来的数据一行也写不回去：
            // 读端产出 `document`、写端找 `value` 正是这一类 bug 的原型。
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
