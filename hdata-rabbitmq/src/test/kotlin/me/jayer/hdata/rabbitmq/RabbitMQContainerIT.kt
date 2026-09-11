package me.jayer.hdata.rabbitmq

import me.jayer.hdata.core.spec.ErrorHandlingSpec
import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Count
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import tools.jackson.databind.node.ObjectNode

/**
 * Covers RabbitMQ client networking with a real containerized broker.
 *
 * Runs only under `-Pintegration-tests` (the `integration` tag excludes it from `mvn test`).
 */
@Tag("integration")
class RabbitMQContainerIT {

    @Test
    fun `writes and reads messages through a RabbitMQ container`() {
        GenericContainer<Nothing>(DockerImageName.parse("rabbitmq:3.13-alpine")).apply {
            withExposedPorts(AMQP_PORT)
            withStartupAttempts(3)
        }.use { rabbitmq ->
            rabbitmq.start()
            val host = rabbitmq.host
            val port = rabbitmq.getMappedPort(AMQP_PORT)

            // Write messages.
            val writeSchema = Schema.builder()
                .addStringField("body")
                .addStringField("routing_key")
                .addStringField("exchange")
                .build()
            val rows = listOf(
                Row.withSchema(writeSchema).addValues("hello", "test-q", "").build(),
                Row.withSchema(writeSchema).addValues("world", "test-q", "").build(),
            )
            Pipeline.create().also { pipeline ->
                val input = pipeline.apply(Create.of(rows).withRowSchema(writeSchema))
                PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(
                    RabbitMQWriteProvider().from(
                        config(
                            "WriteToRabbitMQ",
                            """
                            host: "$host"
                            port: $port
                            queue: test-q
                            body_field: body
                            routing_key_field: routing_key
                            declare_queue: true
                            """.trimIndent(),
                        )
                    )
                )
                pipeline.run().waitUntilFinish()
            }

            // Read messages back.
            Pipeline.create().also { pipeline ->
                val output = PCollectionRowTuple.empty(pipeline).apply(
                    RabbitMQReadProvider().from(
                        config(
                            "ReadFromRabbitMQ",
                            """
                            host: "$host"
                            port: $port
                            queue: test-q
                            max_messages: 10
                            """.trimIndent(),
                        )
                    )
                ).get(Tags.MAIN_OUTPUT)
                PAssert.that(output).satisfies { result ->
                    val readRows = result.toList()
                    val bodies = readRows.map { it.getString("body") }.toSet()
                    assert(bodies == setOf("hello", "world")) { "Expected hello and world, got: $bodies" }
                    val routingKeys = readRows.map { it.getString("routing_key") }.toSet()
                    assert(routingKeys == setOf("test-q")) { "Expected routing key test-q, got: $routingKeys" }
                    null
                }
                pipeline.run().waitUntilFinish()
            }
        }
    }

    @Test
    fun `dead letter captures a failed write when error_handling is configured`() {
        GenericContainer<Nothing>(DockerImageName.parse("rabbitmq:3.13-alpine")).apply {
            withExposedPorts(AMQP_PORT)
            withStartupAttempts(3)
        }.use { rabbitmq ->
            rabbitmq.start()
            val host = rabbitmq.host
            val port = rabbitmq.getMappedPort(AMQP_PORT)

            // Write a row without the body field — it should go to dead letter.
            val badSchema = Schema.builder().addStringField("key").build()
            val rows = listOf(Row.withSchema(badSchema).addValue("k1").build())

            Pipeline.create().also { pipeline ->
                val input = pipeline.apply(Create.of(rows).withRowSchema(badSchema))
                val out = PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(
                    RabbitMQWriteProvider().from(
                        config(
                            "WriteToRabbitMQ",
                            """
                            host: "$host"
                            port: $port
                            queue: test-q
                            error_handling:
                              output: errors
                            """.trimIndent(),
                            withErrorHandling = true,
                        )
                    )
                )
                PAssert.thatSingleton(out.get(Tags.ERROR_OUTPUT).apply(Count.globally())).isEqualTo(1L)
                pipeline.run().waitUntilFinish()
            }
        }
    }

    private fun config(name: String, yaml: String, withErrorHandling: Boolean = false) =
        TransformConfig(
            name,
            SpecMappers.YAML.readTree(yaml) as ObjectNode,
            if (withErrorHandling) ErrorHandlingSpec(output = "errors") else null,
        )

    private companion object {
        const val AMQP_PORT = 5672
    }
}
