package me.jayer.hdata.kafka

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.DeliveryCapabilities
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.OrderingScope
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.kafka.transform.KafkaWriteFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `WriteToKafka`: write to Kafka asynchronously in batches, with dead-letter output.
 *
 * @author wuya
 */
class KafkaWriteProvider : TypedTransformProvider<KafkaWriteConfig>(KafkaWriteConfig::class.java) {

    override fun identifier(): String = "WriteToKafka"

    override fun description(): String = "Write to Kafka asynchronously in batches, with dead-letter output"

    override fun outputCollectionNames(): List<String> = listOf(Tags.ERROR_OUTPUT)

    override fun deliveryCapabilities(config: TransformConfig): DeliveryCapabilities {
        val atLeastOnce = config.bind(KafkaWriteConfig::class.java).sinkDeliveryGuarantee == KafkaWriteConfig.AT_LEAST_ONCE
        return DeliveryCapabilities(
            deliveryMode = if (atLeastOnce) DeliveryMode.AT_LEAST_ONCE else DeliveryMode.AT_MOST_ONCE,
            replayBehavior = ReplayBehavior.NOT_APPLICABLE,
            ordering = OrderingScope.PER_KEY,
            requiresIdempotencyKey = atLeastOnce,
            notes = if (atLeastOnce) {
                "sink_delivery_guarantee=at-least-once (acks=all): a retry after an unconfirmed send can " +
                    "duplicate a record. exactly-once is not implemented (errors explicitly)."
            } else {
                "sink_delivery_guarantee=none (acks=0, fire-and-forget): an unconfirmed send can also be lost " +
                    "on a broker failure, so this is at-most-once, not at-least-once."
            },
        )
    }

    override fun create(
        config: KafkaWriteConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return KafkaSink(config, context.errorHandling != null, context.transformName)
    }
}

private class KafkaSink(
    private val config: KafkaWriteConfig,
    private val deadLetter: Boolean,
    private val transformName: String,
) : RowSink() {

    override fun write(input: PCollection<Row>): PCollection<Row>? {
        val errorSchema = ErrorSchemas.of(input.schema)
        val errors = input
            .apply("Write", ParDo.of(KafkaWriteFn(config, errorSchema, deadLetter, transformName)))
            .setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
