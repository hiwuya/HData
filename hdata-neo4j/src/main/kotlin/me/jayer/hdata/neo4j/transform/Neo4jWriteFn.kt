package me.jayer.hdata.neo4j.transform

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.neo4j.Neo4jWriteConfig
import me.jayer.hdata.neo4j.internal.DriverFactory
import me.jayer.hdata.neo4j.internal.RealDriverFactory
import me.jayer.hdata.neo4j.internal.buildParams
import me.jayer.hdata.neo4j.internal.newSession
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.neo4j.driver.Driver
import org.slf4j.LoggerFactory

/**
 * 写入 Neo4j：执行 `statement`（Cypher），把行字段绑定成 `$param`。
 *
 * 攒够 `batch_size` 行（或 bundle 结束）在**一个事务里**一次提交。
 * 之前是每来一行就开一个 session、跑一条语句、再关掉——`batch_size` 收下就丢掉，
 * 每行一次网络往返加一次事务提交，吞吐被压到很低。
 *
 * 批量提交失败时退回**逐条**写，这样才能定位到具体是哪一行坏了
 * （与 `WriteToJdbc` 的处理方式一致）：写不进去的那几条按 `error_handling`
 * 进死信，没开死信就直接抛、作业失败。
 *
 * 连接在每个 DoFn 实例里独立建立（`@Setup` 建、`@Teardown` 关），字段标记 `@Transient` 保证可序列化。
 *
 * @author wuya
 */
class Neo4jWriteFn(
    private val config: Neo4jWriteConfig,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
    private val driverFactory: DriverFactory = RealDriverFactory,
) : DoFn<Row, Row>() {

    @Transient
    private var driver: Driver? = null

    @Transient
    private var buffered: MutableList<Pending>? = null

    @Transient
    private var failures: MutableList<ValueInSingleWindow<Row>>? = null

    private class Pending(val record: ValueInSingleWindow<Row>, val params: Map<String, Any?>)

    @Setup
    fun setup() {
        driver = driverFactory.create(config)
        buffered = mutableListOf()
        failures = mutableListOf()
    }

    @Teardown
    fun tearDown() {
        runCatching { driver?.close() }
        driver = null
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
        // 参数绑定失败（行里缺字段）是这一行自己的问题，不该拖累同批的其他行
        val params = try {
            buildParams(row, config.statement, config.parameters)
        } catch (e: Exception) {
            reject(record, e)
            return
        }
        val queue = checkNotNull(buffered) { "写入器未初始化" }
        queue.add(Pending(record, params))
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
        try {
            writeBatch(queue)
            RECORDS_WRITTEN.inc(queue.size.toLong())
        } catch (e: Exception) {
            LOGGER.warn("Neo4j 批量写入失败，退回逐条写入以定位坏数据: {}", e.message)
            writeOneByOne(queue)
        } finally {
            queue.clear()
        }
    }

    /** 一个事务跑完整批：要么全进去，要么整批回滚后由 [writeOneByOne] 重来。 */
    private fun writeBatch(queue: List<Pending>) {
        val d = driver ?: driverFactory.create(config).also { driver = it }
        newSession(d, config).use { session ->
            val tx = session.beginTransaction()
            try {
                queue.forEach { tx.run(config.statement, it.params) }
                tx.commit()
            } catch (e: Exception) {
                runCatching { tx.rollback() }
                throw e
            } finally {
                runCatching { tx.close() }
            }
        }
    }

    private fun writeOneByOne(queue: List<Pending>) {
        val d = driver ?: driverFactory.create(config).also { driver = it }
        newSession(d, config).use { session ->
            queue.forEach { pending ->
                try {
                    session.run(config.statement, pending.params)
                    RECORDS_WRITTEN.inc()
                } catch (e: Exception) {
                    reject(pending.record, e)
                }
            }
        }
    }

    private fun reject(record: ValueInSingleWindow<Row>, e: Exception) {
        if (!deadLetter) {
            throw e
        }
        LOGGER.warn("写入 Neo4j 失败，转入死信: {}", e.message)
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
        private val LOGGER = LoggerFactory.getLogger(Neo4jWriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(Neo4jWriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(Neo4jWriteFn::class.java, "records_rejected")
    }
}
