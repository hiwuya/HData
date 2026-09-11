package me.jayer.hdata.rabbitmq

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.rabbitmq.transform.RABBITMQ_READ_SCHEMA
import me.jayer.hdata.rabbitmq.transform.RabbitMQReadFn
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `ReadFromRabbitMQ`: reads messages from a RabbitMQ queue as a bounded snapshot.
 *
 * Uses synchronous `basicGet` to pull up to [RabbitMQReadConfig.maxMessages] messages. The output
 * schema is fixed (see [RABBITMQ_READ_SCHEMA]). For streaming consumption, configure a high
 * `max_messages` with a large `wait_timeout_ms`, or extend this connector with an SDF in the
 * future.
 *
 * @author wuya
 */
class RabbitMQReadProvider : TypedTransformProvider<RabbitMQReadConfig>(RabbitMQReadConfig::class.java) {

    override fun identifier(): String = "ReadFromRabbitMQ"

    override fun description(): String = "Read messages from a RabbitMQ queue (bounded snapshot)"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(
        config: RabbitMQReadConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return RabbitMQSource(config)
    }
}

private class RabbitMQSource(private val config: RabbitMQReadConfig) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        // The DoFn expects a single trigger element (Any); the message loop is inside processElement.
        return begin
            .apply("Trigger", Create.of(listOf(1)))
            .apply("ReadFromRabbitMQ", ParDo.of(RabbitMQReadFn(config)))
            .setRowSchema(RABBITMQ_READ_SCHEMA)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
