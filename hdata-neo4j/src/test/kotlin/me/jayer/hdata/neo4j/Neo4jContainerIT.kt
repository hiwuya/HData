package me.jayer.hdata.neo4j

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
import tools.jackson.databind.node.ObjectNode

/** Covers Bolt transactions and result mapping against a live Neo4j server. */
@Tag("integration")
class Neo4jContainerIT {
    @Test
    fun `writes nodes and reads them through Bolt`() {
        GenericContainer<Nothing>(DockerImageName.parse("neo4j:5.26-community")).apply {
            withEnv("NEO4J_AUTH", "neo4j/$PASSWORD")
            withExposedPorts(PORT)
        }.use { neo4j ->
            neo4j.start()
            val uri = "bolt://${neo4j.host}:${neo4j.getMappedPort(PORT)}"
            val schema = Schema.builder().addStringField("id").addStringField("name").build()
            val rows = listOf(Row.withSchema(schema).addValues("a", "alpha").build(), Row.withSchema(schema).addValues("b", "beta").build())
            Pipeline.create().also { pipeline ->
                val input = pipeline.apply(Create.of(rows).withRowSchema(schema))
                PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(Neo4jWriteProvider().from(config("WriteToNeo4j", "uri: \"$uri\"\nuser: neo4j\npassword: \"$PASSWORD\"\nstatement: \"CREATE (:Order {id: ${'$'}id, name: ${'$'}name})\"\nbatch_size: 2")))
                pipeline.run().waitUntilFinish()
            }
            Pipeline.create().also { pipeline ->
                val output = PCollectionRowTuple.empty(pipeline).apply(Neo4jReadProvider().from(config("ReadFromNeo4j", "uri: \"$uri\"\nuser: neo4j\npassword: \"$PASSWORD\"\nquery: \"MATCH (o:Order) RETURN o.id AS id, o.name AS name\"\nschema_fields: [\"id:STRING\", \"name:STRING\"]"))).get(Tags.MAIN_OUTPUT)
                PAssert.that(output).containsInAnyOrder(rows)
                pipeline.run().waitUntilFinish()
            }
        }
    }

    private fun config(name: String, yaml: String) = TransformConfig(name, SpecMappers.YAML.readTree(yaml) as ObjectNode)
    private companion object { const val PORT = 7687; const val PASSWORD = "hdata-password" }
}
