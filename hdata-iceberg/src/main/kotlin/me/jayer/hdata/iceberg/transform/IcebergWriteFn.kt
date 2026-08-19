package me.jayer.hdata.iceberg.transform

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.iceberg.IcebergWriteConfig
import me.jayer.hdata.iceberg.internal.IcebergCatalogs
import me.jayer.hdata.iceberg.internal.parseSchemaFields
import me.jayer.hdata.iceberg.internal.rowToRecord
import me.jayer.hdata.iceberg.internal.schemaOf
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.apache.iceberg.Table
import org.apache.iceberg.data.Record
import org.apache.iceberg.hadoop.HadoopCatalog
import org.slf4j.LoggerFactory

/**
 * 写入 Iceberg：每个 bundle 累积的行落成一个 AVRO 数据文件再提交（append）。
 *
 * 目标表结构由 `schema_fields` 声明，表不存在则自动创建。
 * 单条行转不成 Iceberg 记录时**只有那一条**进死信（转换在 `@ProcessElement` 做，
 * 不是攒到 bundle 末尾才一起转，否则一条坏数据会把整个 bundle 拖进死信）；
 * 落盘/提交失败才把缓冲的全部行转入死信。
 *
 * 死信记录一律带**原始行自己的时间戳与窗口**：现编 `Instant.now()` + `GlobalWindow`
 * 的话既没法重放，在窗口化的 pipeline 里 `context.output` 还会直接抛异常。
 *
 * @author wuya
 */
class IcebergWriteFn(
    private val config: IcebergWriteConfig,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
) : DoFn<Row, Row>() {

    @Transient
    private var catalog: HadoopCatalog? = null

    @Transient
    private var table: Table? = null

    @Transient
    private var icebergSchema: org.apache.iceberg.Schema? = null

    @Transient
    private var fields: List<Pair<String, Schema.FieldType>>? = null

    @Transient
    private var failures: MutableList<ValueInSingleWindow<Row>>? = null

    /** 缓冲里同时留着原始行与转好的记录：落盘失败时死信要用原始行的时间戳与窗口。 */
    @Transient
    private var buffer: MutableList<Pending>? = null

    private class Pending(val record: ValueInSingleWindow<Row>, val converted: Record)

    @Setup
    fun setup() {
        catalog = IcebergCatalogs.openCatalog(config.warehouse, config.catalogName)
        val declaredSchema = schemaOf(config.schemaFields)
        table = IcebergCatalogs.ensureTable(catalog!!, config.table, declaredSchema)
        // 外部创建的表字段 ID 通常与 schemaOf 从 1 生成的 ID 不同。数据文件必须使用表自己的
        // schema/字段 ID，否则文件看似提交成功，读取时却会把列映射错。
        icebergSchema = table!!.schema()
        // schema_fields 的解析结果只算一次，别在逐行热路径上反复 split
        fields = parseSchemaFields(config.schemaFields)
        failures = mutableListOf()
        buffer = mutableListOf()
    }

    @Teardown
    fun teardown() {
        runCatching { catalog?.close() }
        catalog = null
    }

    @StartBundle
    fun startBundle() {
        buffer?.clear()
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
        val converted = try {
            rowToRecord(checkNotNull(icebergSchema) { "Iceberg schema 未初始化" }, row, checkNotNull(fields))
        } catch (e: Exception) {
            reject(record, e)
            return
        }
        checkNotNull(buffer) { "写入器未初始化" }.add(Pending(record, converted))
    }

    @FinishBundle
    fun finishBundle(context: FinishBundleContext) {
        val pending = checkNotNull(buffer)
        if (pending.isNotEmpty()) {
            try {
                IcebergCatalogs.writeRecords(checkNotNull(table), pending.map { it.converted }, checkNotNull(icebergSchema))
                RECORDS_WRITTEN.inc(pending.size.toLong())
            } catch (e: Exception) {
                if (!deadLetter) throw e
                LOGGER.warn("写入 Iceberg 失败，缓冲的 {} 行转入死信: {}", pending.size, e.message)
                pending.forEach { reject(it.record, e) }
            }
            pending.clear()
        }
        val rejected = checkNotNull(failures)
        rejected.forEach { context.output(it.value, it.timestamp, it.window) }
        rejected.clear()
    }

    private fun reject(record: ValueInSingleWindow<Row>, e: Exception) {
        if (!deadLetter) throw e
        LOGGER.warn("写入 Iceberg 失败，转入死信: {}", e.message)
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
        private val LOGGER = LoggerFactory.getLogger(IcebergWriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(IcebergWriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(IcebergWriteFn::class.java, "records_rejected")
    }
}
