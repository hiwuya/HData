package me.jayer.hdata.hbase

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.DeliveryCapabilities
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.OrderingScope
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.hbase.transform.HBaseWriteFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `WriteToHBase` performs batch HBase writes with dead-letter support.
 */
class HBaseWriteProvider : TypedTransformProvider<HBaseWriteConfig>(HBaseWriteConfig::class.java) {

    override fun identifier(): String = "WriteToHBase"

    override fun description(): String = "Batch write HBase with dead-letter support"

    override fun supportTier(): ConnectorSupportTier = ConnectorSupportTier.LOGIC_TESTED_ONLY

    override fun deliveryCapabilities(config: TransformConfig) = DeliveryCapabilities(
        DeliveryMode.AT_LEAST_ONCE, ReplayBehavior.NOT_APPLICABLE, OrderingScope.NONE,
        notes = "HBase Put is idempotent for a deterministic row key and values: a retried Put replaces the same cells. Concurrent writers still follow HBase last-write-wins semantics.",
    )

    override fun outputCollectionNames(): List<String> = listOf(Tags.ERROR_OUTPUT)

    override fun create(
        config: HBaseWriteConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return HBaseSink(config, context.errorHandling != null, context.transformName)
    }
}

private class HBaseSink(
    private val config: HBaseWriteConfig,
    private val deadLetter: Boolean,
    private val transformName: String,
) : RowSink() {

    override fun write(input: PCollection<Row>): PCollection<Row>? {
        val errorSchema = ErrorSchemas.of(input.schema)
        val codec = HBaseRowCodec.of(config.rowkeyField, config.rowkeyFormat, config.schemaFields, config.family)
        val errors = input
            .apply("Write", ParDo.of(HBaseWriteFn(config, codec, errorSchema, deadLetter, transformName)))
            .setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
