package me.jayer.hdata.hbase.transform

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.hbase.HBaseConnections
import me.jayer.hdata.hbase.HBaseRowCodec
import me.jayer.hdata.hbase.HBaseWriteConfig
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Table
import org.slf4j.LoggerFactory

/**
 * 攒批写入 HBase，支持死信输出。
 *
 * 提交走 `Table.batch(actions, results)` 而不是 `Table.put(list)`：`put` 失败时只能拿到一个笼统的
 * 异常，重构前的代码因此把**整批**记录都标成失败，而且死信里的 `element` 全是 null、时间戳与窗口
 * 是现编的 `Instant.now()` + GlobalWindow——既没法重放，在窗口化的 pipeline 里还会直接抛异常。
 * `batch` 的 `results[i]` 能精确到行：成功是 `Result`，失败是 `Throwable`，null 表示没轮到它。
 *
 * @author wuya
 */
class HBaseWriteFn(
    private val config: HBaseWriteConfig,
    private val codec: HBaseRowCodec,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
) : DoFn<Row, Row>() {

    @Transient
    private var connection: Connection? = null

    @Transient
    private var table: Table? = null

    @Transient
    private var buffered: MutableList<Pending>? = null

    @Transient
    private var failures: MutableList<ValueInSingleWindow<Row>>? = null

    private class Pending(val record: ValueInSingleWindow<Row>, val put: Put)

    @Setup
    fun setup() {
        buffered = mutableListOf()
        failures = mutableListOf()
        val conn = HBaseConnections.newConnection(config.configuration())
        connection = conn
        // Table 是轻量的，但每次 flush 都新建一个仍然是白白的开销，这里跟连接同生命周期
        table = conn.getTable(TableName.valueOf(config.table))
    }

    @Teardown
    fun tearDown() {
        runCatching { table?.close() }
        runCatching { connection?.close() }
        table = null
        connection = null
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
        val put = try {
            codec.toPut(row)
        } catch (e: Exception) {
            reject(record, e)
            return
        }
        val queue = checkNotNull(buffered) { "写入器未初始化" }
        queue.add(Pending(record, put))
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
        val results = arrayOfNulls<Any>(queue.size)
        var batchError: Exception? = null
        try {
            checkNotNull(table).batch(queue.map { it.put }, results)
        } catch (e: Exception) {
            // 部分失败时 batch 也会抛，但 results 已经填好了，逐行看结果比看这个异常准
            batchError = e
        }
        queue.forEachIndexed { index, pending ->
            when (val result = results[index]) {
                is Throwable -> reject(pending.record, result.asException())
                // null 表示这一行根本没被尝试（整批在提交前就挂了）
                null -> reject(pending.record, batchError ?: IllegalStateException("HBase 未返回这一行的写入结果"))
                else -> RECORDS_WRITTEN.inc()
            }
        }
        queue.clear()
    }

    private fun Throwable.asException(): Exception = this as? Exception ?: RuntimeException(this)

    private fun reject(record: ValueInSingleWindow<Row>, e: Exception) {
        if (!deadLetter) {
            throw e
        }
        LOGGER.warn("写入 HBase 失败，转入死信: {}", e.message)
        RECORDS_REJECTED.inc()
        checkNotNull(failures).add(
            ValueInSingleWindow.of(
                // 保留原始行与它自己的时间戳/窗口，才谈得上重放
                ErrorSchemas.failure(errorSchema, record.value, e, transformName),
                record.timestamp,
                record.window,
                record.paneInfo,
            )
        )
    }


    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(HBaseWriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(HBaseWriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(HBaseWriteFn::class.java, "records_rejected")
    }
}
