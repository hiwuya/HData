package me.jayer.hdata.dynamodb.internal

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import java.net.URI
import java.time.Duration
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration

/**
 * Factory for creating [DynamoDbClient] instances from configuration.
 *
 * @author wuya
 */
object DynamoDBClients {

    fun newClient(
        region: String,
        endpointOverride: String = "",
        accessKeyId: String = "",
        secretAccessKey: String = "",
    ): DynamoDbClient {
        val builder = DynamoDbClient.builder()
            .region(Region.of(region))
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .apiCallTimeout(Duration.ofSeconds(60))
                    .apiCallAttemptTimeout(Duration.ofSeconds(15))
                    .build()
            )

        if (endpointOverride.isNotBlank()) {
            builder.endpointOverride(URI.create(endpointOverride))
        }

        if (accessKeyId.isNotBlank() && secretAccessKey.isNotBlank()) {
            builder.credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKeyId, secretAccessKey)
                )
            )
        }
        // When no explicit credentials are provided, the SDK uses its default chain
        // (env vars, instance profile, ECS task role, etc.).

        return builder.build()
    }
}
