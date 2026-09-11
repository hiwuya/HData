package me.jayer.hdata.mongodb

import com.mongodb.client.MongoClients
import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.bson.Document
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import tools.jackson.databind.node.ObjectNode
import kotlin.test.assertEquals

/** Verifies MongoDB write and partitioned read operations against a live server. */
@Tag("integration")
class MongoContainerIT {
    @Test
    fun `writes and reads documents through MongoDB`() {
        GenericContainer<Nothing>(DockerImageName.parse("mongo:7.0")).apply { withExposedPorts(PORT) }.use { mongo ->
            mongo.start()
            val uri = "mongodb://${mongo.host}:${mongo.getMappedPort(PORT)}"
            val schema = Schema.builder().addStringField("id").addDoubleField("amount").build()
            val rows = listOf(Row.withSchema(schema).addValues("a", 1.5).build(), Row.withSchema(schema).addValues("b", 2.5).build())
            Pipeline.create().also { pipeline ->
                val input = pipeline.apply(Create.of(rows).withRowSchema(schema))
                PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(MongoWriteProvider().from(config("WriteToMongoDb", "connection_uri: \"$uri\"\ndatabase: hdata\ncollection: orders\nschema_fields: [\"id:STRING\", \"amount:DOUBLE\"]")))
                pipeline.run().waitUntilFinish()
            }
            MongoClients.create(uri).use { client -> assertEquals(2L, client.getDatabase("hdata").getCollection("orders").countDocuments()) }
            Pipeline.create().also { pipeline ->
                val output = PCollectionRowTuple.empty(pipeline).apply(MongoReadProvider().from(config("ReadFromMongoDb", "connection_uri: \"$uri\"\ndatabase: hdata\ncollection: orders\nschema_fields: [\"id:STRING\", \"amount:DOUBLE\"]\npartition_num: 1"))).get(Tags.MAIN_OUTPUT)
                PAssert.that(output).containsInAnyOrder(rows)
                pipeline.run().waitUntilFinish()
            }
        }
    }

    private fun config(name: String, yaml: String) = TransformConfig(name, SpecMappers.YAML.readTree(yaml) as ObjectNode)
    private companion object { const val PORT = 27017 }
}
