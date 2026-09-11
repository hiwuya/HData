package me.jayer.hdata.pulsar

import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import tools.jackson.databind.node.ObjectNode
import java.time.Duration

/**
 * Exercises the Pulsar connector against a real standalone broker.
 *
 * Pulsar's topic lookup protocol always hands the client back a broker URL built from
 * `advertisedAddress` and `brokerServicePort`, even when the client is already talking to the
 * only broker there is. The default advertised address is the container's own hostname, which
 * is unreachable from the test JVM, so the broker port is fixed 1:1 (host == container) and the
 * advertised address is forced to `localhost` to keep both hops resolvable.
 */
@Tag("integration")
class PulsarContainerIT {
    @Test
    fun `writes and reads a bounded topic snapshot`() {
        PulsarBrokerContainer().use { broker ->
            broker.start()
            val serviceUrl = "pulsar://localhost:$BROKER_PORT"
            val topic = "persistent://public/default/hdata-it"
            val inputSchema = Schema.builder().addByteArrayField("value").build()
            val rows = listOf("one", "two").map { Row.withSchema(inputSchema).addValue(it.toByteArray()).build() }

            Pipeline.create().also { pipeline ->
                val input = pipeline.apply(Create.of(rows).withRowSchema(inputSchema))
                PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(
                    PulsarWriteProvider().from(config("WriteToPulsar", "service_url: \"$serviceUrl\"\ntopic: \"$topic\"")),
                )
                pipeline.run().waitUntilFinish()
            }

            val outputSchema = Schema.builder().addStringField("topic").addByteArrayField("value").build()
            Pipeline.create().also { pipeline ->
                val output = PCollectionRowTuple.empty(pipeline).apply(
                    PulsarReadProvider().from(config("ReadFromPulsar", "service_url: \"$serviceUrl\"\ntopic: \"$topic\"\nmax_messages: 2")),
                ).get(Tags.MAIN_OUTPUT)
                PAssert.that(output).containsInAnyOrder(rows.map { Row.withSchema(outputSchema).addValues(topic, requireNotNull(it.getBytes("value"))).build() })
                pipeline.run().waitUntilFinish()
            }
        }
    }

    private fun config(name: String, yaml: String): TransformConfig =
        TransformConfig(name, me.jayer.hdata.core.spec.SpecMappers.YAML.readTree(yaml) as ObjectNode)

    private class PulsarBrokerContainer :
        GenericContainer<PulsarBrokerContainer>(DockerImageName.parse("docker.io/apachepulsar/pulsar:3.3.7")) {
        init {
            addFixedExposedPort(BROKER_PORT, BROKER_PORT)
            withCommand("bin/pulsar", "standalone", "--no-functions-worker", "--no-stream-storage", "--advertised-address", "localhost")
            waitingFor(Wait.forLogMessage(".*messaging service is ready.*\\n", 1).withStartupTimeout(Duration.ofMinutes(3)))
        }
    }

    private companion object {
        const val BROKER_PORT = 6650
    }
}
