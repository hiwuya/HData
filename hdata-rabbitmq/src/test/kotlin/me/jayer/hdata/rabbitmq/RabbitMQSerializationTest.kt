package me.jayer.hdata.rabbitmq

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.rabbitmq.transform.RABBITMQ_READ_SCHEMA
import me.jayer.hdata.rabbitmq.transform.RabbitMQReadFn
import me.jayer.hdata.rabbitmq.transform.RabbitMQWriteFn
import org.apache.beam.sdk.util.SerializableUtils
import org.junit.jupiter.api.Test

/**
 * A DoFn that captures a non-serializable object only blows up when the job is submitted; a unit
 * test that only calls `processElement` would never catch it, so every connector has at least one
 * `ensureSerializable` assertion.
 */
class RabbitMQSerializationTest {

    @Test
    fun `DoFns are serializable`() {
        SerializableUtils.ensureSerializable(
            RabbitMQReadFn(RabbitMQReadConfig())
        )
        SerializableUtils.ensureSerializable(
            RabbitMQWriteFn(
                RabbitMQWriteConfig(),
                ErrorSchemas.of(RABBITMQ_READ_SCHEMA),
                deadLetter = true,
                transformName = "WriteToRabbitMQ",
            )
        )
    }
}
