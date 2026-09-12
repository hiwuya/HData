package me.jayer.hdata.elasticsearch8

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.core.spi.DeliveryCapabilities
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.OrderingScope
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.elasticsearch8.transform.EsWriteFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `WriteToElasticsearch8` writes buffered bulk requests with dead-letter support.
 */
class EsWriteProvider : TypedTransformProvider<EsWriteConfig>(EsWriteConfig::class.java) {

    override fun identifier(): String = "WriteToElasticsearch8"

    override fun description(): String = "Bulk write to Elasticsearch 8.x with dead-letter support"

    override fun supportTier(): ConnectorSupportTier = ConnectorSupportTier.EXPERIMENTAL

    override fun deliveryCapabilities(config: TransformConfig): DeliveryCapabilities = DeliveryCapabilities(
        deliveryMode = DeliveryMode.AT_LEAST_ONCE, replayBehavior = ReplayBehavior.NOT_APPLICABLE,
        ordering = OrderingScope.NONE, requiresIdempotencyKey = true,
        notes = "Bulk index operations can repeat after an ambiguous response. The current writer does not assign document IDs, so upstream must provide a deduplication boundary.",
    )

    override fun outputCollectionNames(): List<String> = listOf(Tags.ERROR_OUTPUT)

    override fun create(
        config: EsWriteConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return EsSink(config, context.errorHandling != null, context.transformName)
    }
}

private class EsSink(
    private val config: EsWriteConfig,
    private val deadLetter: Boolean,
    private val transformName: String,
) : RowSink() {

    override fun write(input: PCollection<Row>): PCollection<Row>? {
        val inputSchema = input.schema
        val errorSchema = ErrorSchemas.of(inputSchema)
        val errors = input
            .apply("Write", ParDo.of(EsWriteFn(config, inputSchema, errorSchema, deadLetter, transformName)))
            .setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
