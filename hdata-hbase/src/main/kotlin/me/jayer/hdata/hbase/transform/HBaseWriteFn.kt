package me.jayer.hdata.hbase.transform

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.hbase.HBaseConnections
import me.jayer.hdata.hbase.encodeCell
import me.jayer.hdata.hbase.parseSchemaFields
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
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory

/**
 * 逐条（攒批）写入 HBase，支持死信输出。
 *
 * 每条输入行用 [rowkeyField] 做 rowkey，按 [schemaFields] 写入 [family] 列族；攒够 [batchSize] 后
 * 批量 `table.put`。写失败且开了死信时转入死信流，否则异常直接抛出，作业失败。
 */
class HBaseWriteFn(
    private val zookeeperQuorum: String,
    private val table: String,
    private val rowkeyField: String,
    private val family: String,
    private val schemaFields: List<String>?,
    private val batchSize: Int,
    private val inputSchema: Schema,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
) : DoFn<Row, Row>() {

    @Transient
    private var connection: Connection? = null

    private val fields = parseSchemaFields(schemaFields)
    private var familyBytes: ByteArray = ByteArray(0)

    private val buffered = mutableListOf<Put>()
    private val failures = mutableListOf<ValueInSingleWindow<Row>>()

    @Setup
    fun setup() {
        connection = HBaseConnections.newConnection(zookeeperQuorum)
        familyBytes = Bytes.toBytes(family)
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
        pane: PaneInfo,
    ) {
        try {
            buffered.add(rowToPut(row))
            if (buffered.size >= batchSize) {
                flush()
            }
        } catch (e: Exception) {
            if (!deadLetter) {
                throw e
            }
            LOGGER.warn("构造写入 HBase 的 Put 失败，转入死信: {}", e.message)
            RECORDS_REJECTED.inc()
            failures.add(ValueInSingleWindow.of(ErrorSchemas.failure(errorSchema, row, e, transformName), timestamp, window, pane))
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
        runCatching { connection?.close() }
        connection = null
    }

    private fun flush() {
        if (buffered.isEmpty()) {
            return
        }
        val conn = checkNotNull(connection) { "HBase 连接未初始化" }
        val table = conn.getTable(TableName.valueOf(this.table))
        try {
            table.put(buffered)
            RECORDS_WRITTEN.inc(buffered.size.toLong())
        } catch (e: Exception) {
            if (!deadLetter) {
                throw e
            }
            LOGGER.warn("批量写入 HBase 失败，转入死信: {}", e.message)
            buffered.forEach { put ->
                RECORDS_REJECTED.inc()
                failures.add(
                    ValueInSingleWindow.of(
                        ErrorSchemas.failure(errorSchema, null, e, transformName),
                        org.joda.time.Instant.now(),
                        org.apache.beam.sdk.transforms.windowing.GlobalWindow.INSTANCE,
                        PaneInfo.NO_FIRING,
                    )
                )
            }
        } finally {
            runCatching { table.close() }
            buffered.clear()
        }
    }

    private fun rowToPut(row: Row): Put {
        val rowkey = checkNotNull(row.getValue(rowkeyField)) { "写入 HBase 的行缺少 $rowkeyField 字段" }
        val put = Put(Bytes.toBytes(rowkey.toString()))
        for (field in fields) {
            val raw = row.getValue<Any?>(field.name)
            val bytes = encodeCell(field.normalizedType, raw)
            if (bytes != null) {
                put.addColumn(familyBytes, Bytes.toBytes(field.name), bytes)
            }
        }
        return put
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(HBaseWriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(HBaseWriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(HBaseWriteFn::class.java, "records_rejected")
    }
}
