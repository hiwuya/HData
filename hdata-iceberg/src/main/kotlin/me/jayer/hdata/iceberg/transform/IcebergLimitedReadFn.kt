package me.jayer.hdata.iceberg.transform

import me.jayer.hdata.iceberg.IcebergReadConfig
import me.jayer.hdata.iceberg.internal.IcebergCatalogs
import me.jayer.hdata.iceberg.internal.recordToRow
import me.jayer.hdata.iceberg.internal.validateReadableSchema
import me.jayer.hdata.iceberg.parseIcebergFilter
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.apache.iceberg.Table
import org.apache.iceberg.data.IcebergGenerics
import org.apache.iceberg.hadoop.HadoopCatalog

/**
 * Iceberg's global LIMIT read path. A single trigger element is scanned sequentially by one worker over the current
 * snapshot, spanning any number of data files, and stops after actually producing [IcebergReadConfig.limit] matching
 * records. GenericReader also applies equality/position deletes, avoiding a "read only the first file" that would
 * fall short of the limit or return deleted records.
 */
class IcebergLimitedReadFn(
    private val config: IcebergReadConfig,
    private val schema: Schema,
    private val schemaFields: List<Pair<String, Schema.FieldType>>,
) : DoFn<String, Row>() {

    @Transient private var catalog: HadoopCatalog? = null
    @Transient private var table: Table? = null

    @Setup
    fun setup() {
        catalog = IcebergCatalogs.openCatalog(config.warehouse, config.catalogName, config.hadoopConf)
        table = IcebergCatalogs.loadTable(checkNotNull(catalog), config.table)
        validateReadableSchema(checkNotNull(table).schema(), schemaFields, config.table)
    }

    @Teardown
    fun teardown() {
        runCatching { catalog?.close() }
        catalog = null
        table = null
    }

    @ProcessElement
    fun processElement(receiver: OutputReceiver<Row>) {
        val t = checkNotNull(table) { "Iceberg table is not initialized" }
        var builder = IcebergGenerics.read(t)
        if (config.filter.isNotBlank()) builder = builder.where(parseIcebergFilter(config.filter))
        builder.build().use { records ->
            val iterator = records.iterator()
            var emitted = 0L
            while (emitted < config.limit && iterator.hasNext()) {
                receiver.output(recordToRow(schema, iterator.next(), schemaFields))
                emitted++
            }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
