package me.jayer.hdata.kafka

import me.jayer.hdata.core.HData
import me.jayer.hdata.core.spec.PipelineSpecLoader
import me.jayer.hdata.core.spec.SpecMappers
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName

/** Exercises Beam's Kafka SDF with a real Kafka-protocol-compatible broker. */
@Tag("integration")
class KafkaBrokerIT {
    @Test
    fun `reads a bounded topic from a real broker`() {
        RedpandaContainer().use { broker ->
            broker.start()
            val properties = mapOf("bootstrap.servers" to BOOTSTRAP)
            AdminClient.create(properties).use { it.createTopics(listOf(NewTopic(TOPIC, 1, 1))).all().get() }
            KafkaProducer<String, String>(properties + mapOf(
                "key.serializer" to StringSerializer::class.java.name,
                "value.serializer" to StringSerializer::class.java.name,
            )).use { producer ->
                producer.send(ProducerRecord(TOPIC, "one", "alpha")).get()
                producer.send(ProducerRecord(TOPIC, "two", "beta")).get()
            }
            val spec = """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromKafka
                      config:
                        bootstrap_servers: "$BOOTSTRAP"
                        topics: ["$TOPIC"]
                        scan_startup_mode: earliest-offset
                        scan_bounded_mode: latest-offset
                    - type: MapToFields
                      config:
                        fields:
                          key: key
                          value: value
                    - type: AssertEqual
                      config:
                        elements:
                          - { key: one, value: alpha }
                          - { key: two, value: beta }
            """.trimIndent()
            HData(PipelineSpecLoader.parse(spec, SpecMappers.YAML, "kafka-it"))
                .build(PipelineOptionsFactory.fromArgs("--runner=DirectRunner").create())
                .first.run().waitUntilFinish()
        }
    }

    private class RedpandaContainer : GenericContainer<RedpandaContainer>(DockerImageName.parse("redpandadata/redpanda:v24.1.6")) {
        init {
            addFixedExposedPort(PORT, 9092)
            withCommand("redpanda", "start", "--overprovisioned", "--smp", "1", "--memory", "512M", "--reserve-memory", "0M", "--node-id", "0", "--check=false", "--kafka-addr", "PLAINTEXT://0.0.0.0:9092", "--advertise-kafka-addr", "PLAINTEXT://$BOOTSTRAP")
        }
    }

    private companion object {
        const val PORT = 19092
        const val BOOTSTRAP = "localhost:19092"
        const val TOPIC = "hdata-it-orders"
    }
}
