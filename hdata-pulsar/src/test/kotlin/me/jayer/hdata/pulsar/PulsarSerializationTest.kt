package me.jayer.hdata.pulsar

import me.jayer.hdata.core.error.ErrorSchemas
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.util.SerializableUtils
import org.junit.jupiter.api.Test

/**
 * A DoFn that captures a non-serializable field only fails when the pipeline is submitted;
 * calling `processElement` in a unit test never exercises that path, so every connector
 * asserts `ensureSerializable` on its DoFns directly (see the hdata-core convention).
 */
class PulsarSerializationTest {
    @Test
    fun `DoFns are serializable`() {
        SerializableUtils.ensureSerializable(
            PulsarReadFn(PulsarReadConfig(serviceUrl = "pulsar://localhost:6650", topic = "orders")),
        )
        val schema = Schema.builder().addByteArrayField("value").build()
        SerializableUtils.ensureSerializable(
            PulsarWriteFn(
                PulsarWriteConfig(serviceUrl = "pulsar://localhost:6650", topic = "orders"),
                ErrorSchemas.of(schema),
                deadLetter = true,
                transformName = "WriteToPulsar",
            ),
        )
    }
}
