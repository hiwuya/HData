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
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.KeyType
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.PutRequest
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType
import software.amazon.awssdk.services.dynamodb.model.WriteRequest
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

                // Write rows via DynamoDB SDK.
                client.batchWriteItem(
                    BatchWriteItemRequest.builder()
                        .requestItems(
                            mapOf(
                                "test-table" to listOf(
                                    WriteRequest.builder().putRequest(
                                        PutRequest.builder().item(
                                            mapOf(
                                                "id" to AttributeValue.builder().s("1").build(),
                                                "name" to AttributeValue.builder().s("alice").build(),
                                                "value" to AttributeValue.builder().n("1.5").build(),
                                            )
                                        ).build(),
                                    ).build(),
                                    WriteRequest.builder().putRequest(
                                        PutRequest.builder().item(
                                            mapOf(
                                                "id" to AttributeValue.builder().s("2").build(),
                                                "name" to AttributeValue.builder().s("bob").build(),
                                                "value" to AttributeValue.builder().n("2.5").build(),
                                            )
                                        ).build(),
                                    ).build(),
                                )
                            )
                        )
                        .build()
                )

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

    private fun config(name: String, yaml: String, withErrorHandling: Boolean = false) =
        TransformConfig(
            name,
            SpecMappers.YAML.readTree(yaml) as tools.jackson.databind.node.ObjectNode,
            if (withErrorHandling) ErrorHandlingSpec(output = "errors") else null,
        )

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
