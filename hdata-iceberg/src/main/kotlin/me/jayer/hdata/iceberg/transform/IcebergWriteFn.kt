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
 * Writes to Iceberg: each bundle's accumulated rows are written into one AVRO data file and then committed (append).
 *
 * The target table structure is declared via `schema_fields`, and the table is auto-created if it does not exist.
 * When a single row cannot be converted into an Iceberg record, **only that row** goes to the dead letter (the
 * conversion is done in `@ProcessElement`, not batched and converted together at the end of the bundle, otherwise one
 * bad record would drag the whole bundle into the dead letter); only a write/commit failure sends all buffered rows
 * to the dead letter.
 *
 * Dead-letter records always carry **the original row's own timestamp and window**: fabricating `Instant.now()` +
 * `GlobalWindow` is neither replayable, and in a windowed pipeline `context.output` would throw outright.
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

    /** The buffer holds both the original row and the converted record: on a write failure the dead letter needs the original row's timestamp and window. */
    @Transient
    private var buffer: MutableList<Pending>? = null

    private class Pending(val record: ValueInSingleWindow<Row>, val converted: Record)

    @Setup
    fun setup() {
        catalog = IcebergCatalogs.openCatalog(config.warehouse, config.catalogName)
        val declaredSchema = schemaOf(config.schemaFields)
        table = IcebergCatalogs.ensureTable(catalog!!, config.table, declaredSchema)
        // An externally created table's field IDs usually differ from the IDs schemaOf generates starting at 1. The
        // data file must use the table's own schema/field IDs, otherwise the file appears to commit successfully but
        // the columns get mapped wrong on read.
        icebergSchema = table!!.schema()
        // The parse result of schema_fields is computed once; do not re-split it repeatedly on the per-row hot path.
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
            rowToRecord(checkNotNull(icebergSchema) { "Iceberg schema is not initialized" }, row, checkNotNull(fields))
        } catch (e: Exception) {
            reject(record, e)
            return
        }
        checkNotNull(buffer) { "Writer is not initialized" }.add(Pending(record, converted))
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
                LOGGER.warn("Write to Iceberg failed, sending {} buffered rows to the dead letter: {}", pending.size, e.message)
                pending.forEach { reject(it.record, e) }
            } finally {
                pending.clear()
            }
        }
        val rejected = checkNotNull(failures)
        rejected.forEach { context.output(it.value, it.timestamp, it.window) }
        rejected.clear()
    }

    private fun reject(record: ValueInSingleWindow<Row>, e: Exception) {
        if (!deadLetter) throw e
        LOGGER.warn("Write to Iceberg failed, sending to the dead letter: {}", e.message)
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
