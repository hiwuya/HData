package me.jayer.hdata.sqs

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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest
import software.amazon.awssdk.services.sqs.model.SendMessageRequest
import tools.jackson.databind.node.ObjectNode
import java.net.URI

/**
 * Covers SQS client networking with a real LocalStack container.
 *
 * Runs only under `-Pintegration-tests`.
 */
@Tag("integration")
class SQSContainerIT {

    @Test
    fun `writes and reads messages through a LocalStack container`() {
        GenericContainer<Nothing>(DockerImageName.parse("localstack/localstack:3.8")).apply {
            withExposedPorts(PORT)
            withEnv("SERVICES", "sqs")
            withStartupAttempts(3)
            // The default port-open wait strategy races LocalStack's internal service init; a
            // request can land on the edge port before the SQS backend is actually loaded.
            waitingFor(org.testcontainers.containers.wait.strategy.Wait.forLogMessage(".*Ready\\.\n", 1))
        }.use { localstack ->
            localstack.start()
            val endpoint = "http://${localstack.host}:${localstack.getMappedPort(PORT)}"

            // Create queue via SDK.
            val client = buildSqsClient(endpoint)
            try {
                val queueUrl = client.createQueue(
                    CreateQueueRequest.builder().queueName("test-queue").build()
                ).queueUrl()

                // Write via HData provider.
                val writeSchema = Schema.builder().addStringField("body").build()
                val rows = listOf(
                    Row.withSchema(writeSchema).addValues("msg-0").build(),
                    Row.withSchema(writeSchema).addValues("msg-1").build(),
                    Row.withSchema(writeSchema).addValues("msg-2").build(),
                )
                Pipeline.create().also { pipeline ->
                    val input = pipeline.apply(Create.of(rows).withRowSchema(writeSchema))
                    PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(
                        SQSWriteProvider().from(
                            config(
                                "WriteToSQS",
                                """
                                queue_url: $queueUrl
                                endpoint_override: $endpoint
                                region: us-east-1
                                access_key_id: test
                                secret_access_key: test
                                """.trimIndent(),
                            )
                        )
                    )
                    pipeline.run().waitUntilFinish()
                }

                // Read back via HData provider.
                Pipeline.create().also { pipeline ->
                    val output = PCollectionRowTuple.empty(pipeline).apply(
                        SQSReadProvider().from(
                            config(
                                "ReadFromSQS",
                                """
                                queue_url: $queueUrl
                                endpoint_override: $endpoint
                                region: us-east-1
                                access_key_id: test
                                secret_access_key: test
                                max_messages: 5
                                wait_time_seconds: 2
                                """.trimIndent(),
                            )
                        )
                    ).get(Tags.MAIN_OUTPUT)
                    PAssert.that(output).satisfies { result ->
                        val readRows = result.toList()
                        assert(readRows.size == 3) { "Expected 3 messages, got: ${readRows.size}" }
                        val bodies = readRows.map { it.getString("body") }.toSet()
                        assert(bodies == setOf("msg-0", "msg-1", "msg-2")) {
                            "Expected msg-0/1/2, got: $bodies"
                        }
                        null
                    }
                    pipeline.run().waitUntilFinish()
                }
            } finally {
                client.close()
            }
        }
    }

    @Test
    fun `dead letter captures a failed write`() {
        GenericContainer<Nothing>(DockerImageName.parse("localstack/localstack:3.8")).apply {
            withExposedPorts(PORT)
            withEnv("SERVICES", "sqs")
            withStartupAttempts(3)
            // The default port-open wait strategy races LocalStack's internal service init; a
            // request can land on the edge port before the SQS backend is actually loaded.
            waitingFor(org.testcontainers.containers.wait.strategy.Wait.forLogMessage(".*Ready\\.\n", 1))
        }.use { localstack ->
            localstack.start()
            val endpoint = "http://${localstack.host}:${localstack.getMappedPort(PORT)}"

            val client = buildSqsClient(endpoint)
            try {
                val queueUrl = client.createQueue(
                    CreateQueueRequest.builder().queueName("dl-queue").build()
                ).queueUrl()

                // Write a row missing the body field — should go to dead letter.
                val badSchema = Schema.builder().addStringField("extra").build()
                val rows = listOf(Row.withSchema(badSchema).addValues("value").build())

                Pipeline.create().also { pipeline ->
                    val input = pipeline.apply(Create.of(rows).withRowSchema(badSchema))
                    val out = PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(
                        SQSWriteProvider().from(
                            config(
                                "WriteToSQS",
                                """
                                queue_url: $queueUrl
                                endpoint_override: $endpoint
                                region: us-east-1
                                access_key_id: test
                                secret_access_key: test
                                body_field: body
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
            } finally {
                client.close()
            }
        }
    }

    private fun config(name: String, yaml: String, withErrorHandling: Boolean = false): TransformConfig {
        val node = SpecMappers.YAML.readTree(yaml) as ObjectNode
        // The real pipeline loader strips error_handling out of the config node before binding
        // (PipelineGraphBuilder.extractErrorHandling); this test helper bypasses that loader, so it
        // must strip it here too, or Jackson rejects it as an unrecognized property.
        if (withErrorHandling) node.remove(ErrorHandlingSpec.CONFIG_KEY)
        return TransformConfig(
            name,
            node,
            if (withErrorHandling) ErrorHandlingSpec(output = "errors") else null,
        )
    }

    private fun buildSqsClient(endpoint: String): SqsClient {
        return SqsClient.builder()
            .endpointOverride(URI.create(endpoint))
            .region(Region.US_EAST_1)
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))
            )
            .build()
    }

    private companion object {
        const val PORT = 4566
    }
}
