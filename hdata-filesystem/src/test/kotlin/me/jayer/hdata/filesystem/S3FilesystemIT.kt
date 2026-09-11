package me.jayer.hdata.filesystem

import me.jayer.hdata.core.spec.SpecMappers
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
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import tools.jackson.databind.node.ObjectNode
import java.net.URI

/** Exercises S3A through a real MinIO server; Docker is required. */
@Tag("integration")
class S3FilesystemIT {

    @Test
    fun `s3a text write and read round trip`() {
        MinioContainer().use { minio ->
                minio.start()
                val endpoint = "http://${minio.host}:${minio.getMappedPort(9000)}"
                S3Client.builder()
                    .endpointOverride(URI.create(endpoint))
                    .region(Region.US_EAST_1)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                    .forcePathStyle(true)
                    .build().use { it.createBucket { request -> request.bucket(BUCKET) } }

                val config = """
                    path: "s3a://$BUCKET/output"
                    default_fs: "s3a://$BUCKET"
                    hadoop_conf:
                      fs.s3a.endpoint: "$endpoint"
                      fs.s3a.access.key: "$ACCESS_KEY"
                      fs.s3a.secret.key: "$SECRET_KEY"
                      fs.s3a.path.style.access: "true"
                      fs.s3a.connection.ssl.enabled: "false"
                """.trimIndent()
                val schema = FilesystemSchemas.TEXT_SCHEMA
                val rows = listOf("alpha", "beta").map { Row.withSchema(schema).addValue(it).build() }
                val write = Pipeline.create()
                val input = write.apply(Create.of(rows).withRowSchema(schema))
                PCollectionRowTuple.of(Tags.MAIN_INPUT, input)
                    .apply(FilesystemWriteProvider().from(transformConfig(config)))
                write.run().waitUntilFinish()

                val read = Pipeline.create()
                val output = PCollectionRowTuple.empty(read)
                    .apply(FilesystemReadProvider().from(transformConfig(config.replace("output\"", "output/*\""))))
                    .get(Tags.MAIN_OUTPUT)
                PAssert.that(output).containsInAnyOrder(rows)
                read.run().waitUntilFinish()
            }
    }

    private fun transformConfig(yaml: String): TransformConfig =
        TransformConfig("s3", SpecMappers.YAML.readTree(yaml) as ObjectNode)

    private companion object {
        const val ACCESS_KEY = "hdata-test-access"
        const val SECRET_KEY = "hdata-test-secret"
        const val BUCKET = "hdata-test"
    }

    private class MinioContainer : GenericContainer<MinioContainer>(
        DockerImageName.parse("minio/minio:RELEASE.2025-04-22T22-12-26Z"),
    ) {
        init {
            withEnv("MINIO_ROOT_USER", ACCESS_KEY)
            withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
            withCommand("server", "/data")
            withExposedPorts(9000)
        }
    }
}
