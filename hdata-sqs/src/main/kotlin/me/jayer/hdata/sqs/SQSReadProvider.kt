package me.jayer.hdata.sqs

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.core.spi.DeliveryCapabilities
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.OrderingScope
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.core.spi.SourceMode
import me.jayer.hdata.sqs.transform.SQSReadFn
import me.jayer.hdata.sqs.transform.SQS_READ_SCHEMA
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `ReadFromSQS`: reads messages from an Amazon SQS queue as a batch or stream via long-polling.
 *
 * @author wuya
 */
class SQSReadProvider : TypedTransformProvider<SQSReadConfig>(SQSReadConfig::class.java) {

    override fun identifier(): String = "ReadFromSQS"

    override fun description(): String = "Read messages from Amazon SQS as a batch or stream"

    override fun supportTier(): ConnectorSupportTier = ConnectorSupportTier.EXPERIMENTAL

    override fun deliveryCapabilities(config: TransformConfig): DeliveryCapabilities {
        val read = config.bind(SQSReadConfig::class.java)
        return DeliveryCapabilities(
            deliveryMode = if (read.deleteAfterRead) DeliveryMode.AT_MOST_ONCE else DeliveryMode.AT_LEAST_ONCE,
            replayBehavior = ReplayBehavior.NOT_APPLICABLE, ordering = OrderingScope.NONE,
            notes = if (read.deleteAfterRead) "Messages are deleted after source output, before downstream completion, so a crash can lose them."
            else "Messages remain visible after their visibility timeout and can be redelivered; no durable consumer cursor exists.",
        )
    }

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun sourceMode(config: TransformConfig): SourceMode =
        if (config.bind(SQSReadConfig::class.java).streaming) SourceMode.UNBOUNDED else SourceMode.BOUNDED

    override fun create(
        config: SQSReadConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return SQSSource(config)
    }
}

private class SQSSource(private val config: SQSReadConfig) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        return begin
            .apply("Trigger", Create.of(listOf(1)))
            .apply("ReadFromSQS", ParDo.of(SQSReadFn(config)))
            .setRowSchema(SQS_READ_SCHEMA)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
