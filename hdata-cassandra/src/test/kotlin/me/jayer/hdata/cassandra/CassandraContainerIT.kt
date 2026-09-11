package me.jayer.hdata.cassandra

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
import tools.jackson.databind.node.ObjectNode

/**
 * Covers Cassandra client networking with a real containerized server.
 *
 * Runs only under `-Pintegration-tests`.
 */
@Tag("integration")
class CassandraContainerIT {

    @Test
    fun `writes and reads rows through a Cassandra container`() {
        GenericContainer<Nothing>(DockerImageName.parse("cassandra:4.1")).apply {
            withExposedPorts(CQL_PORT)
            withStartupAttempts(3)
            // Cassandra 4.x sizes its heap and off-heap pools off the *host's* total memory, not
            // the cgroup limit — left at its default it tries to allocate several GB and gets
            // OOM-killed under any container memory cap below several GB. Cap the JVM heap
            // explicitly (matches the container's memory limit) instead of raising the limit to
            // match Cassandra's uncapped default.
            withEnv("MAX_HEAP_SIZE", "512M")
            withEnv("HEAP_NEWSIZE", "128M")
            withCreateContainerCmdModifier { cmd ->
                cmd.hostConfig!!.withMemory(1500L * 1024 * 1024)
            }
            // Cassandra 4.1 takes well over a minute to open the CQL port (gossip settling +
            // default-role creation); the default 60s port-wait times out mid-startup every time.
            withStartupTimeout(java.time.Duration.ofSeconds(180))
        }.use { cassandra ->
            cassandra.start()
            val endpoint = "${cassandra.host}:${cassandra.getMappedPort(CQL_PORT)}"

            // Wait for Cassandra to be ready.
            waitForCassandra(endpoint)

            // Create keyspace and table.
            executeQuery(endpoint, """
                CREATE KEYSPACE IF NOT EXISTS test_ks
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """.trimIndent())
            executeQuery(endpoint, """
                CREATE TABLE IF NOT EXISTS test_ks.events (
                    id int PRIMARY KEY,
                    name text,
                    value double
                )
            """.trimIndent())

            // Write rows.
            val writeSchema = Schema.builder()
                .addInt32Field("id")
                .addStringField("name")
                .addDoubleField("value")
                .build()
            val rows = listOf(
                Row.withSchema(writeSchema).addValues(1, "alice", 1.5).build(),
                Row.withSchema(writeSchema).addValues(2, "bob", 2.5).build(),
            )
            Pipeline.create().also { pipeline ->
                val input = pipeline.apply(Create.of(rows).withRowSchema(writeSchema))
                PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(
                    CassandraWriteProvider().from(
                        config(
                            "WriteToCassandra",
                            """
                            endpoints: ["$endpoint"]
                            keyspace: test_ks
                            table: events
                            """.trimIndent(),
                        )
                    )
                )
                pipeline.run().waitUntilFinish()
            }

            // Read rows back.
            Pipeline.create().also { pipeline ->
                val output = PCollectionRowTuple.empty(pipeline).apply(
                    CassandraReadProvider().from(
                        config(
                            "ReadFromCassandra",
                            """
                            endpoints: ["$endpoint"]
                            keyspace: test_ks
                            query: "SELECT id, name, value FROM test_ks.events"
                            """.trimIndent(),
                        )
                    )
                ).get(Tags.MAIN_OUTPUT)
                PAssert.that(output).satisfies { result ->
                    val readRows = result.toList()
                    assert(readRows.size == 2) { "Expected 2 rows, got: ${readRows.size}" }
                    // Cassandra can only ORDER BY within a single partition, so a full scan with no
                    // WHERE clause (as here) can't request server-side ordering — sort client-side.
                    val names = readRows.mapNotNull { it.getString("name") }.sorted()
                    assert(names == listOf("alice", "bob")) { "Expected [alice, bob], got: $names" }
                    null
                }
                pipeline.run().waitUntilFinish()
            }
        }
    }

    @Test
    fun `dead letter captures a failed write`() {
        GenericContainer<Nothing>(DockerImageName.parse("cassandra:4.1")).apply {
            withExposedPorts(CQL_PORT)
            withStartupAttempts(3)
            withEnv("MAX_HEAP_SIZE", "512M")
            withEnv("HEAP_NEWSIZE", "128M")
            withCreateContainerCmdModifier { cmd ->
                cmd.hostConfig!!.withMemory(1500L * 1024 * 1024)
            }
            // Cassandra 4.1 takes well over a minute to open the CQL port (gossip settling +
            // default-role creation); the default 60s port-wait times out mid-startup every time.
            withStartupTimeout(java.time.Duration.ofSeconds(180))
        }.use { cassandra ->
            cassandra.start()
            val endpoint = "${cassandra.host}:${cassandra.getMappedPort(CQL_PORT)}"
            waitForCassandra(endpoint)

            executeQuery(endpoint, """
                CREATE KEYSPACE IF NOT EXISTS test_ks_dl
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """.trimIndent())
            executeQuery(endpoint, """
                CREATE TABLE IF NOT EXISTS test_ks_dl.bad_rows (
                    id int PRIMARY KEY,
                    name text
                )
            """.trimIndent())

            // Write a row with a string where int is expected — it should go to dead letter.
            val badSchema = Schema.builder()
                .addStringField("id")
                .addStringField("name")
                .build()
            val rows = listOf(Row.withSchema(badSchema).addValues("not-a-number", "test").build())

            Pipeline.create().also { pipeline ->
                val input = pipeline.apply(Create.of(rows).withRowSchema(badSchema))
                val out = PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(
                    CassandraWriteProvider().from(
                        config(
                            "WriteToCassandra",
                            """
                            endpoints: ["$endpoint"]
                            keyspace: test_ks_dl
                            table: bad_rows
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
        }
    }

    private fun config(name: String, yaml: String, withErrorHandling: Boolean = false): TransformConfig {
        val node = SpecMappers.YAML.readTree(yaml) as ObjectNode
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

    private fun executeQuery(endpoint: String, cql: String) {
        val host = endpoint.substringBefore(":")
        val port = endpoint.substringAfter(":").toInt()
        val session = com.datastax.oss.driver.api.core.CqlSession.builder()
            .addContactPoint(java.net.InetSocketAddress(host, port))
            .withLocalDatacenter("datacenter1")
            .build()
        try {
            session.execute(cql)
        } finally {
            session.close()
        }
    }

    private fun waitForCassandra(endpoint: String) {
        val host = endpoint.substringBefore(":")
        val port = endpoint.substringAfter(":").toInt()
        val maxAttempts = 30
        repeat(maxAttempts) { attempt ->
            try {
                val session = com.datastax.oss.driver.api.core.CqlSession.builder()
                    .addContactPoint(java.net.InetSocketAddress(host, port))
                    .withLocalDatacenter("datacenter1")
                    .build()
                session.close()
                return
            } catch (e: Exception) {
                if (attempt < maxAttempts - 1) {
                    Thread.sleep(2000)
                } else {
                    throw RuntimeException("Cassandra not ready after $maxAttempts attempts", e)
                }
            }
        }
    }

    private companion object {
        const val CQL_PORT = 9042
    }
}
