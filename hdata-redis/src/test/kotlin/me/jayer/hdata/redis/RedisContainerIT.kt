package me.jayer.hdata.redis

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.redis.transform.REDIS_KEY_VALUE_SCHEMA
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
import tools.jackson.databind.node.ObjectNode

/** Covers Redis client networking with a real containerized server. */
@Tag("integration")
class RedisContainerIT {
    @Test
    fun `writes and scans values through a Redis container`() {
        GenericContainer<Nothing>(DockerImageName.parse("redis:7.4-alpine")).apply { withExposedPorts(PORT) }.use { redis ->
            redis.start()
            val config = "host: \"${redis.host}\"\nport: ${redis.getMappedPort(PORT)}"
            val schema = Schema.builder().addStringField("key").addStringField("value").build()
            val rows = listOf("alpha" to "one", "beta" to "two").map { (key, value) -> Row.withSchema(schema).addValues(key, value).build() }
            Pipeline.create().also { pipeline ->
                val input = pipeline.apply(Create.of(rows).withRowSchema(schema))
                PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(RedisWriteProvider().from(config("WriteToRedis", "$config\nmode: set")))
                pipeline.run().waitUntilFinish()
            }
            Pipeline.create().also { pipeline ->
                val output = PCollectionRowTuple.empty(pipeline).apply(RedisReadProvider().from(config("ReadFromRedis", "$config\nmode: scan\nkey_pattern: \"*\""))).get(Tags.MAIN_OUTPUT)
                PAssert.that(output).containsInAnyOrder(rows.map {
                    Row.withSchema(REDIS_KEY_VALUE_SCHEMA)
                        .addValues(requireNotNull(it.getString("key")), requireNotNull(it.getString("value")))
                        .build()
                })
                pipeline.run().waitUntilFinish()
            }
        }
    }

    private fun config(name: String, yaml: String) = TransformConfig(name, SpecMappers.YAML.readTree(yaml) as ObjectNode)

    private companion object { const val PORT = 6379 }
}
