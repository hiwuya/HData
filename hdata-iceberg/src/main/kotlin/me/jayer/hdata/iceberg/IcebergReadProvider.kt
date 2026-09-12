package me.jayer.hdata.iceberg

import me.jayer.hdata.core.spi.DeliveryCapabilities
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.OrderingScope
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.iceberg.AggregateCombineFn
import me.jayer.hdata.iceberg.AggregateToRowFn
import me.jayer.hdata.iceberg.PartialAgg
import me.jayer.hdata.iceberg.aggregateSchema
import me.jayer.hdata.iceberg.internal.IcebergCatalogs
import me.jayer.hdata.iceberg.internal.parseSchemaFields
import me.jayer.hdata.iceberg.parseAggregations
import me.jayer.hdata.iceberg.transform.IcebergAggregateEnumeratorFn
import me.jayer.hdata.iceberg.transform.IcebergReadFileFn
import me.jayer.hdata.iceberg.transform.IcebergLimitedReadFn
import me.jayer.hdata.iceberg.transform.IcebergSplitEnumeratorFn
import me.jayer.hdata.iceberg.transform.IcebergFileSplit
import org.apache.beam.sdk.transforms.Combine
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.coders.SerializableCoder
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `ReadFromIceberg`: scans the entire Iceberg table and maps each row to a Row.
 *
 * @author wuya
 */
class IcebergReadProvider : TypedTransformProvider<IcebergReadConfig>(IcebergReadConfig::class.java) {

    override fun identifier(): String = "ReadFromIceberg"

    override fun description(): String = "Read from Iceberg"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun deliveryCapabilities(config: TransformConfig): DeliveryCapabilities = DeliveryCapabilities(
        deliveryMode = DeliveryMode.AT_LEAST_ONCE,
        replayBehavior = ReplayBehavior.FULL_REPLAY,
        ordering = OrderingScope.NONE,
        notes = "Scans the table's current snapshot at read time with no persisted read position (not an " +
            "incremental read from a prior snapshot id); a full job restart re-scans the then-current snapshot, " +
            "which may already differ if the table was written to between runs.",
    )

    override fun create(config: IcebergReadConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return IcebergSource(config)
    }
}

private class IcebergSource(private val config: IcebergReadConfig) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        // Push-down aggregation: COUNT/MIN/MAX come from the data files' metadata statistics, reading no data at
        // all, and output a single aggregated row.
        if (config.aggregations.isNotEmpty()) {
            val specs = parseAggregations(config.aggregations)
            val outSchema = IcebergCatalogs.openCatalog(config.warehouse, config.catalogName, config.hadoopConf).use { catalog ->
                val table = IcebergCatalogs.loadTable(catalog, config.table)
                // Partition-column values are recorded in the manifest, not in the data files, and the aggregation
                // enumerator does not backfill partitions — so reject partitioned tables explicitly at
                // graph-construction time, rather than letting the user hit an obscure read error.
                require(!table.spec().isPartitioned()) {
                    "Push-down aggregation does not yet support partitioned table [${config.table}]: partition columns are not in the data files, so per-file local aggregation is impossible; use a plain read instead"
                }
                aggregateSchema(specs, table)
            }
            val trigger = begin.apply("Trigger", Create.of(listOf("")))
            val partials = trigger.apply("EnumerateAgg", ParDo.of(IcebergAggregateEnumeratorFn(config, specs)))
            partials.setCoder(SerializableCoder.of(PartialAgg::class.java))
            val merged = partials.apply("MergeAgg", Combine.globally(AggregateCombineFn(specs)))
            merged.setCoder(SerializableCoder.of(PartialAgg::class.java))
            return merged.apply("ToRow", ParDo.of(AggregateToRowFn(specs, outSchema))).setRowSchema(outSchema)
        }
        val schemaFields = parseSchemaFields(config.schemaFields)
        val schema = config.outputSchema()
        val trigger = begin.apply("Trigger", Create.of(listOf("")))
        if (config.limit > 0) {
            return trigger.apply(
                "ReadLimited",
                ParDo.of(IcebergLimitedReadFn(config, schema, schemaFields)),
            ).setRowSchema(schema)
        }
        // First enumerate data files into splits (units of parallelism), then read files in parallel.
        val splits = trigger.apply("EnumerateSplits", ParDo.of(IcebergSplitEnumeratorFn(config)))
        splits.setCoder(SerializableCoder.of(IcebergFileSplit::class.java))
        return splits.apply("ReadFiles", ParDo.of(IcebergReadFileFn(config, schema, schemaFields))).setRowSchema(schema)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
