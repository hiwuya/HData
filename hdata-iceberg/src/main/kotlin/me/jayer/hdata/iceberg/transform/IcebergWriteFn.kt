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
import org.apache.iceberg.hadoop.HadoopCatalog
import org.slf4j.LoggerFactory

/**
 * 写入 Iceberg：每个 bundle 累积的行落成一个 AVRO 数据文件再提交（append）。
 *
 * 目标表结构由 `schema_fields` 声明，表不存在则自动创建。单条失败进死信，
 * 整个 bundle 落盘失败则把缓冲的全部行转入死信。
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
    private var failures: MutableList<ValueInSingleWindow<Row>>? = null

    @Transient
    private var buffer: MutableList<Row>? = null

    @Setup
    fun setup() {
        catalog = IcebergCatalogs.openCatalog(config.warehouse, config.catalogName)
        icebergSchema = schemaOf(config.schemaFields)
        table = IcebergCatalogs.ensureTable(catalog!!, config.table, icebergSchema!!)
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
    }

    @ProcessElement
    fun processElement(
        @Element row: Row,
        @Timestamp timestamp: org.joda.time.Instant,
        window: BoundedWindow,
        pane: PaneInfo,
        receiver: OutputReceiver<Row>,
    ) {
        try {
            buffer!!.add(row)
        } catch (e: Exception) {
            reject(ValueInSingleWindow.of(row, timestamp, window, pane), e)
        }
    }

    @FinishBundle
    fun finishBundle(context: FinishBundleContext) {
        try {
            val records = buffer!!.map { rowToRecord(icebergSchema!!, it, parseSchemaFields(config.schemaFields)) }
            IcebergCatalogs.writeRecords(table!!, records, icebergSchema!!)
            RECORDS_WRITTEN.inc(buffer!!.size.toLong())
        } catch (e: Exception) {
            if (!deadLetter) throw e
            LOGGER.warn("写入 Iceberg 失败，缓冲的 {} 行转入死信: {}", buffer!!.size, e.message)
            RECORDS_REJECTED.inc(buffer!!.size.toLong())
            buffer!!.forEach { row ->
                failures!!.add(
                    ValueInSingleWindow.of(
                        ErrorSchemas.failure(errorSchema, row, e, transformName),
                        org.joda.time.Instant.now(),
                        org.apache.beam.sdk.transforms.windowing.GlobalWindow.INSTANCE,
                        PaneInfo.NO_FIRING,
                    )
                )
            }
        }
        failures!!.forEach { context.output(it.value, it.timestamp, it.window) }
        failures!!.clear()
    }

    private fun reject(record: ValueInSingleWindow<Row>, e: Exception) {
        if (!deadLetter) throw e
        LOGGER.warn("写入 Iceberg 失败，转入死信: {}", e.message)
        RECORDS_REJECTED.inc()
        failures!!.add(
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
