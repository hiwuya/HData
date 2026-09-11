package me.jayer.hdata.iceberg

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
import kotlin.test.assertEquals

/**
 * Exercises the HadoopCatalog warehouse against real S3-compatible object storage (MinIO) rather than local disk.
 * A HadoopCatalog commit relies on an atomic rename, which local filesystems provide for free but object stores do
 * not; this is the one Iceberg test that a temp-directory warehouse cannot stand in for.
 */
@Tag("integration")
class IcebergMinioContainerIT {
    @Test
    fun `writes and reads an Iceberg table on S3-compatible storage`() {
        MinioContainer().use { minio ->
            minio.start()
            val endpoint = "http://${minio.host}:${minio.getMappedPort(9000)}"
            S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .forcePathStyle(true)
                .build().use { it.createBucket { request -> request.bucket(BUCKET) } }

            val warehouse = "s3a://$BUCKET/warehouse"
            val hadoopConfYaml = """
                hadoop_conf:
                  fs.s3a.endpoint: "$endpoint"
                  fs.s3a.access.key: "$ACCESS_KEY"
                  fs.s3a.secret.key: "$SECRET_KEY"
                  fs.s3a.path.style.access: "true"
                  fs.s3a.connection.ssl.enabled: "false"
            """.trimIndent()
            val schema = Schema.builder().addInt64Field("id").addStringField("name").build()
            val rows = listOf(
                Row.withSchema(schema).addValues(1L, "alpha").build(),
                Row.withSchema(schema).addValues(2L, "beta").build(),
            )

            Pipeline.create().also { pipeline ->
                val input = pipeline.apply(Create.of(rows).withRowSchema(schema))
                PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(
                    IcebergWriteProvider().from(
                        config(
                            "WriteToIceberg",
                            "warehouse: \"$warehouse\"\ntable: \"db.orders\"\nschema_fields: [\"id:INT64\", \"name:STRING\"]\n$hadoopConfYaml",
                        ),
                    ),
                )
                pipeline.run().waitUntilFinish()
            }

            Pipeline.create().also { pipeline ->
                val output = PCollectionRowTuple.empty(pipeline).apply(
                    IcebergReadProvider().from(
                        config(
                            "ReadFromIceberg",
                            "warehouse: \"$warehouse\"\ntable: \"db.orders\"\nschema_fields: [\"id:INT64\", \"name:STRING\"]\n$hadoopConfYaml",
                        ),
                    ),
                ).get(Tags.MAIN_OUTPUT)
                PAssert.that(output).satisfies { result ->
                    val list = result.toList()
                    assertEquals(setOf(1L to "alpha", 2L to "beta"), list.map { it.getInt64("id") to it.getString("name") }.toSet())
                    null
                }
                pipeline.run().waitUntilFinish()
            }
        }
    }

    private fun config(name: String, yaml: String): TransformConfig =
        TransformConfig(name, SpecMappers.YAML.readTree(yaml) as ObjectNode)

    private class MinioContainer : GenericContainer<MinioContainer>(
        DockerImageName.parse("minio/minio:RELEASE.2025-04-22T22-12-26Z"),
    ) {
        init {
            withEnv("MINIO_ROOT_USER", ACCESS_KEY)
            withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
            withCommand("server", "/data")
            withExposedPorts(9000)
            // The default port-open wait strategy races MinIO's actual S3 API readiness; a client
            // request can land while the port is listening but the server isn't serving yet.
            waitingFor(org.testcontainers.containers.wait.strategy.Wait.forHttp("/minio/health/ready").forStatusCode(200))
        }
    }

    private companion object {
        const val ACCESS_KEY = "hdata-test-access"
        const val SECRET_KEY = "hdata-test-secret"
        const val BUCKET = "hdata-iceberg-test"
    }
}
