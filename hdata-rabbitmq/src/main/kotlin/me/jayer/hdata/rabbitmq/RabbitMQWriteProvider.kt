package me.jayer.hdata.rabbitmq

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.rabbitmq.transform.RabbitMQWriteFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `WriteToRabbitMQ`: writes rows to a RabbitMQ queue with batching, publisher confirms, and
 * dead-letter support.
 *
 * @author wuya
 */
class RabbitMQWriteProvider : TypedTransformProvider<RabbitMQWriteConfig>(RabbitMQWriteConfig::class.java) {

    override fun identifier(): String = "WriteToRabbitMQ"

    override fun description(): String = "Write rows to a RabbitMQ queue with batching and dead-letter output"

    override fun supportTier(): ConnectorSupportTier = ConnectorSupportTier.EXPERIMENTAL

    override fun outputCollectionNames(): List<String> = listOf(Tags.ERROR_OUTPUT)

    override fun create(
        config: RabbitMQWriteConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return RabbitMQSink(config, context.errorHandling != null, context.transformName)
    }
}

private class RabbitMQSink(
    private val config: RabbitMQWriteConfig,
    private val deadLetter: Boolean,
    private val transformName: String,
) : RowSink() {

    override fun write(input: PCollection<Row>): PCollection<Row>? {
        val errorSchema = ErrorSchemas.of(input.schema)
        val errors = input
            .apply("Write", ParDo.of(RabbitMQWriteFn(config, errorSchema, deadLetter, transformName)))
            .setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
