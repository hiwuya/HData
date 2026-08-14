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
 * 单条失败不拖垮整个 bundle，而是按 `error_handling` 决定进死信还是让作业失败。
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
    private var failures: MutableList<ValueInSingleWindow<Row>>? = null

    @Setup
    fun setup() {
        driver = driverFactory.create(config)
        failures = mutableListOf()
    }

    @Teardown
    fun tearDown() {
        runCatching { driver?.close() }
        driver = null
    }

    @StartBundle
    fun startBundle() {
        failures?.clear()
    }

    @ProcessElement
    fun processElement(
        @Element row: Row,
        @Timestamp timestamp: org.joda.time.Instant,
        window: BoundedWindow,
        pane: PaneInfo,
        receiver: OutputReceiver<Row>,
    ) {
        val visw = ValueInSingleWindow.of(row, timestamp, window, pane)
        try {
            val params = buildParams(row, config.statement, config.parameters)
            val d = driver ?: driverFactory.create(config).also { driver = it }
            newSession(d, config).use { session ->
                session.run(config.statement, params)
            }
            RECORDS_WRITTEN.inc()
        } catch (e: Exception) {
            reject(visw, e)
        }
    }

    @FinishBundle
    fun finishBundle(context: FinishBundleContext) {
        checkNotNull(failures).forEach { context.output(it.value, it.timestamp, it.window) }
        failures?.clear()
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
