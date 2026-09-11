package me.jayer.hdata.dynamodb

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
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.KeyType
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType
import java.net.URI

/**
 * Covers DynamoDB client networking with a real DynamoDB Local container.
 *
 * Runs only under `-Pintegration-tests`.
 */
@Tag("integration")
class DynamoDBContainerIT {

    @Test
    fun `writes and reads rows through a DynamoDB container`() {
        GenericContainer<Nothing>(DockerImageName.parse("amazon/dynamodb-local:latest")).apply {
            withExposedPorts(DYNAMO_PORT)
            withCommand("-jar DynamoDBLocal.jar -sharedDb -inMemory")
            withStartupAttempts(3)
            // DynamoDB Local has no health endpoint; an unauthenticated request returning 400
            // (MissingAuthenticationToken) is the standard signal that the HTTP server is actually
            // serving, not just that the port is open.
            waitingFor(org.testcontainers.containers.wait.strategy.Wait.forHttp("/").forStatusCode(400))
        }.use { dynamo ->
            dynamo.start()
            val endpoint = "http://${dynamo.host}:${dynamo.getMappedPort(DYNAMO_PORT)}"

            val client = buildDynamoClient(endpoint)
            try {
                // Create table.
                client.createTable(
                    CreateTableRequest.builder()
                        .tableName("test-table")
                        .keySchema(
                            KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build(),
                        )
                        .attributeDefinitions(
                            AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build(),
                        )
                        .provisionedThroughput(
                            ProvisionedThroughput.builder().readCapacityUnits(5).writeCapacityUnits(5).build(),
                        )
                        .build()
                )

                // Write rows via HData provider.
                val writeSchema = Schema.builder()
                    .addStringField("id")
                    .addStringField("name")
                    .addStringField("value")
                    .build()
                val rows = listOf(
                    Row.withSchema(writeSchema).addValues("1", "alice", "1.5").build(),
                    Row.withSchema(writeSchema).addValues("2", "bob", "2.5").build(),
                )
                Pipeline.create().also { pipeline ->
                    val input = pipeline.apply(Create.of(rows).withRowSchema(writeSchema))
                    PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(
                        DynamoDBWriteProvider().from(
                            config(
                                "WriteToDynamoDB",
                                """
                                table_name: test-table
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

                // Read rows via the HData provider.
                Pipeline.create().also { pipeline ->
                    val output = PCollectionRowTuple.empty(pipeline).apply(
                        DynamoDBReadProvider().from(
                            config(
                                "ReadFromDynamoDB",
                                """
                                table_name: test-table
                                endpoint_override: $endpoint
                                region: us-east-1
                                access_key_id: test
                                secret_access_key: test
                                consistent_read: true
                                """.trimIndent(),
                            )
                        )
                    ).get(Tags.MAIN_OUTPUT)

                    PAssert.that(output).satisfies { result ->
                        val rows = result.toList()
                        assert(rows.size == 2) { "Expected 2 rows, got: ${rows.size}" }
                        val names = rows.map { it.getString("name") ?: "" }.sorted()
                        assert(names == listOf("alice", "bob")) { "Expected [alice, bob], got: $names" }
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
        GenericContainer<Nothing>(DockerImageName.parse("amazon/dynamodb-local:latest")).apply {
            withExposedPorts(DYNAMO_PORT)
            withCommand("-jar DynamoDBLocal.jar -sharedDb -inMemory")
            withStartupAttempts(3)
            // DynamoDB Local has no health endpoint; an unauthenticated request returning 400
            // (MissingAuthenticationToken) is the standard signal that the HTTP server is actually
            // serving, not just that the port is open.
            waitingFor(org.testcontainers.containers.wait.strategy.Wait.forHttp("/").forStatusCode(400))
        }.use { dynamo ->
            dynamo.start()
            val endpoint = "http://${dynamo.host}:${dynamo.getMappedPort(DYNAMO_PORT)}"

            val client = buildDynamoClient(endpoint)
            try {
                // Create table with only "id" and "name" keys.
                client.createTable(
                    CreateTableRequest.builder()
                        .tableName("bad-table")
                        .keySchema(
                            KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build(),
                        )
                        .attributeDefinitions(
                            AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build(),
                        )
                        .provisionedThroughput(
                            ProvisionedThroughput.builder().readCapacityUnits(5).writeCapacityUnits(5).build(),
                        )
                        .build()
                )

                // Write a row where the schema doesn't match (extra fields that DynamoDB accepts fine,
                // but we test dead letter by having the provider fail).
                // Actually, DynamoDB is schemaless — writes won't fail due to extra fields.
                // Instead, we test dead letter by writing to a non-existent table.
                val badSchema = Schema.builder()
                    .addStringField("id")
                    .addStringField("name")
                    .build()
                val rows = listOf(Row.withSchema(badSchema).addValues("1", "test").build())

                Pipeline.create().also { pipeline ->
                    val input = pipeline.apply(Create.of(rows).withRowSchema(badSchema))
                    val out = PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(
                        DynamoDBWriteProvider().from(
                            config(
                                "WriteToDynamoDB",
                                """
                                table_name: non-existent-table
                                endpoint_override: $endpoint
                                region: us-east-1
                                access_key_id: test
                                secret_access_key: test
                                max_retries: 0
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

    @Test
    fun `parallel scan reads all items across segments`() {
        GenericContainer<Nothing>(DockerImageName.parse("amazon/dynamodb-local:latest")).apply {
            withExposedPorts(DYNAMO_PORT)
            withCommand("-jar DynamoDBLocal.jar -sharedDb -inMemory")
            withStartupAttempts(3)
            waitingFor(org.testcontainers.containers.wait.strategy.Wait.forHttp("/").forStatusCode(400))
        }.use { dynamo ->
            dynamo.start()
            val endpoint = "http://${dynamo.host}:${dynamo.getMappedPort(DYNAMO_PORT)}"

            val client = buildDynamoClient(endpoint)
            try {
                client.createTable(
                    CreateTableRequest.builder()
                        .tableName("parallel-table")
                        .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                        .attributeDefinitions(
                            AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build(),
                        )
                        .provisionedThroughput(
                            ProvisionedThroughput.builder().readCapacityUnits(5).writeCapacityUnits(5).build(),
                        )
                        .build()
                )

                val writeSchema = Schema.builder().addStringField("id").build()
                val expectedIds = (1..20).map { "item-$it" }
                val rows = expectedIds.map { Row.withSchema(writeSchema).addValues(it).build() }
                Pipeline.create().also { pipeline ->
                    val input = pipeline.apply(Create.of(rows).withRowSchema(writeSchema))
                    PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(
                        DynamoDBWriteProvider().from(
                            config(
                                "WriteToDynamoDB",
                                """
                                table_name: parallel-table
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

                Pipeline.create().also { pipeline ->
                    val output = PCollectionRowTuple.empty(pipeline).apply(
                        DynamoDBReadProvider().from(
                            config(
                                "ReadFromDynamoDB",
                                """
                                table_name: parallel-table
                                endpoint_override: $endpoint
                                region: us-east-1
                                access_key_id: test
                                secret_access_key: test
                                consistent_read: true
                                parallel_scan_segments: 4
                                """.trimIndent(),
                            )
                        )
                    ).get(Tags.MAIN_OUTPUT)

                    PAssert.that(output).satisfies { result ->
                        val ids = result.map { it.getString("id") }.toList()
                        assert(ids.size == expectedIds.size) {
                            "Expected ${expectedIds.size} items across 4 segments, got ${ids.size}: $ids"
                        }
                        assert(ids.toSet() == expectedIds.toSet()) {
                            "Expected exactly $expectedIds, got $ids (duplicates or gaps across segments)"
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

    private fun config(name: String, yaml: String, withErrorHandling: Boolean = false): TransformConfig {
        val node = SpecMappers.YAML.readTree(yaml) as tools.jackson.databind.node.ObjectNode
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

    private fun buildDynamoClient(endpoint: String): DynamoDbClient {
        return DynamoDbClient.builder()
            .region(Region.US_EAST_1)
            .endpointOverride(URI.create(endpoint))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create("test", "test")
                )
            )
            .build()
    }

    private companion object {
        const val DYNAMO_PORT = 8000
    }
}
