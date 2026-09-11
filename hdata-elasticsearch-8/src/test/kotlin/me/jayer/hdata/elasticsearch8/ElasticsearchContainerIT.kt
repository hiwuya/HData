package me.jayer.hdata.elasticsearch8

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.apache.http.HttpHost
import org.elasticsearch.client.Request
import org.elasticsearch.client.RestClient
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import tools.jackson.databind.node.ObjectNode

/** Verifies Elasticsearch bulk writing and slice-based reads against a live node. */
@Tag("integration")
class ElasticsearchContainerIT {
    @Test
    fun `writes and reads an index through Elasticsearch`() {
        ElasticsearchContainer().use { elasticsearch ->
            elasticsearch.start()
            val uri = "http://${elasticsearch.host}:${elasticsearch.getMappedPort(PORT)}"
            val schema = Schema.builder().addNullableStringField("id").addNullableDoubleField("amount").build()
            val rows = listOf(
                Row.withSchema(schema).addValues("a", 1.5).build(),
                Row.withSchema(schema).addValues("b", 2.5).build(),
            )
            Pipeline.create().also { pipeline ->
                val input = pipeline.apply(Create.of(rows).withRowSchema(schema))
                PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(EsWriteProvider().from(config("WriteToElasticsearch8", "$BASE_CONFIG\nconnection_uri: \"$uri\"")))
                pipeline.run().waitUntilFinish()
            }
            RestClient.builder(HttpHost.create(uri)).build().use { it.performRequest(Request("POST", "/orders/_refresh")) }
            Pipeline.create().also { pipeline ->
                val output = PCollectionRowTuple.empty(pipeline).apply(EsReadProvider().from(config("ReadFromElasticsearch8", "$BASE_CONFIG\nconnection_uri: \"$uri\"\nscan_slices: 2"))).get(Tags.MAIN_OUTPUT)
                PAssert.that(output).containsInAnyOrder(rows)
                pipeline.run().waitUntilFinish()
            }
        }
    }

    private fun config(name: String, yaml: String) = TransformConfig(name, SpecMappers.YAML.readTree(yaml) as ObjectNode)

    private class ElasticsearchContainer : GenericContainer<ElasticsearchContainer>(DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:8.17.0")) {
        init {
            withEnv("discovery.type", "single-node")
            withEnv("xpack.security.enabled", "false")
            withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
            withExposedPorts(PORT)
        }
    }

    private companion object {
        const val PORT = 9200
        const val BASE_CONFIG = "index: orders\nschema_fields: [\"id:STRING\", \"amount:DOUBLE\"]"
    }
}
