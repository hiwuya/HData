package me.jayer.hdata.iceberg

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.core.spi.DeliveryCapabilities
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.OrderingScope
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.iceberg.transform.IcebergTruncateFn
import me.jayer.hdata.iceberg.transform.IcebergWriteFn
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.View
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `WriteToIceberg`: writes rows into an Iceberg table, with dead-letter output support.
 *
 * @author wuya
 */
class IcebergWriteProvider : TypedTransformProvider<IcebergWriteConfig>(IcebergWriteConfig::class.java) {

    override fun identifier(): String = "WriteToIceberg"

    override fun description(): String = "Write to Iceberg"

    override fun outputCollectionNames(): List<String> = listOf(Tags.ERROR_OUTPUT)

    // EXPERIMENTAL: see IcebergReadProvider.supportTier.
    override fun supportTier(): ConnectorSupportTier = ConnectorSupportTier.EXPERIMENTAL

    override fun deliveryCapabilities(config: TransformConfig): DeliveryCapabilities {
        val overwrite = config.bind(IcebergWriteConfig::class.java).mode() == IcebergWriteMode.OVERWRITE
        return DeliveryCapabilities(
            deliveryMode = DeliveryMode.AT_LEAST_ONCE,
            replayBehavior = ReplayBehavior.NOT_APPLICABLE,
            ordering = OrderingScope.NONE,
            requiresIdempotencyKey = !overwrite,
            notes = if (overwrite) {
                "write_mode=overwrite clears the table (via a side input guaranteeing clear-before-write) " +
                    "before committing this run's data, so rerunning the whole job produces the same result."
            } else {
                "write_mode=append: rerunning the whole job commits another full copy of the data on top of " +
                    "what a prior run already committed as a new table snapshot."
            },
        )
    }

    override fun create(config: IcebergWriteConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return IcebergSink(config, context.errorHandling != null, context.transformName)
    }
}

private class IcebergSink(
    private val config: IcebergWriteConfig,
    private val deadLetter: Boolean,
    private val transformName: String,
) : RowSink() {

    override fun write(input: PCollection<Row>): PCollection<Row>? {
        val errorSchema = ErrorSchemas.of(input.schema)
        var write = ParDo.of(IcebergWriteFn(config, errorSchema, deadLetter, transformName))
        if (config.mode() == IcebergWriteMode.OVERWRITE) {
            // The table clear must happen exactly once, before all writes. It is done as a side input: a ParDo with a
            // side input will not process any main input until the side input is fully computed, which is what
            // guarantees the "clear first, then write" ordering (putting it in the write side's @Setup would let a
            // later bundle delete the data an earlier bundle just wrote).
            val truncated = input.pipeline
                .apply("TruncateTrigger", Create.of(""))
                .apply("Truncate", ParDo.of(IcebergTruncateFn(config)))
                .apply("TruncateDone", View.asList())
            write = write.withSideInputs(truncated)
        }
        val errors = input
            .apply("Write", write)
            .setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
