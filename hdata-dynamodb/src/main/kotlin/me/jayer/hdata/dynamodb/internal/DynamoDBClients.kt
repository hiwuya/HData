package me.jayer.hdata.dynamodb.internal

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import java.net.URI

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

        if (endpointOverride.isNotBlank()) {
            builder.endpointOverride(URI.create(endpointOverride))
        }

        if (accessKeyId.isNotBlank() && secretAccessKey.isNotBlank()) {
            builder.credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKeyId, secretAccessKey)
                )
            )
        } else {
            // Use placeholder credentials for local / emulator environments.
            builder.credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create("test", "test")
                )
            )
        }

        return builder.build()
    }
}
