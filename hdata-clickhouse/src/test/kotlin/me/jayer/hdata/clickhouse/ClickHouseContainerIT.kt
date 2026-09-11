package me.jayer.hdata.clickhouse

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
import java.sql.DriverManager

/**
 * Covers ClickHouse client networking with a real containerized server.
 *
 * Runs only under `-Pintegration-tests` (the `integration` tag excludes it from `mvn test`).
 */
@Tag("integration")
class ClickHouseContainerIT {

    @Test
    fun `writes and reads rows through a ClickHouse container`() {
        GenericContainer<Nothing>(DockerImageName.parse("clickhouse/clickhouse-server:24.3-alpine")).apply {
            withExposedPorts(HTTP_PORT)
            withStartupAttempts(3)
        }.use { clickhouse ->
            clickhouse.start()
            val endpoint = "http://${clickhouse.host}:${clickhouse.getMappedPort(HTTP_PORT)}"

            // Create table via JDBC.
            executeQuery(endpoint, """
                CREATE TABLE IF NOT EXISTS default.events (
                    id Int32,
                    name String,
                    value Float64
                ) ENGINE = MergeTree() ORDER BY id
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
                    ClickHouseWriteProvider().from(
                        config(
                            "WriteToClickHouse",
                            """
                            endpoint: "$endpoint"
                            database: default
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
                    ClickHouseReadProvider().from(
                        config(
                            "ReadFromClickHouse",
                            """
                            endpoint: "$endpoint"
                            database: default
                            query: "SELECT id, name, value FROM default.events ORDER BY id"
                            """.trimIndent(),
                        )
                    )
                ).get(Tags.MAIN_OUTPUT)
                PAssert.that(output).satisfies { result ->
                    val readRows = result.toList()
                    assert(readRows.size == 2) { "Expected 2 rows, got: ${readRows.size}" }
                    val names = readRows.map { it.getString("name") }
                    assert(names == listOf("alice", "bob")) { "Expected [alice, bob], got: $names" }
                    null
                }
                pipeline.run().waitUntilFinish()
            }
        }
    }

    @Test
    fun `dead letter captures a row with a type mismatch`() {
        GenericContainer<Nothing>(DockerImageName.parse("clickhouse/clickhouse-server:24.3-alpine")).apply {
            withExposedPorts(HTTP_PORT)
            withStartupAttempts(3)
        }.use { clickhouse ->
            clickhouse.start()
            val endpoint = "http://${clickhouse.host}:${clickhouse.getMappedPort(HTTP_PORT)}"

            executeQuery(endpoint, """
                CREATE TABLE IF NOT EXISTS default.bad_rows (
                    id Int32,
                    name String
                ) ENGINE = MergeTree() ORDER BY id
            """.trimIndent())

            // Write a row with a string where Int32 is expected — it should go to dead letter.
            val badSchema = Schema.builder()
                .addStringField("id")
                .addStringField("name")
                .build()
            val rows = listOf(Row.withSchema(badSchema).addValues("not-a-number", "test").build())

            Pipeline.create().also { pipeline ->
                val input = pipeline.apply(Create.of(rows).withRowSchema(badSchema))
                val out = PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(
                    ClickHouseWriteProvider().from(
                        config(
                            "WriteToClickHouse",
                            """
                            endpoint: "$endpoint"
                            database: default
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

    private fun config(name: String, yaml: String, withErrorHandling: Boolean = false) =
        TransformConfig(
            name,
            SpecMappers.YAML.readTree(yaml) as ObjectNode,
            if (withErrorHandling) ErrorHandlingSpec(output = "errors") else null,
        )

    private fun executeQuery(endpoint: String, sql: String) {
        val uri = java.net.URI(endpoint)
        val host = uri.host ?: "localhost"
        val port = if (uri.port > 0) uri.port else 8123
        val jdbcUrl = "jdbc:clickhouse://$host:$port/default"
        val conn = DriverManager.getConnection(jdbcUrl, "default", "")
        try {
            conn.createStatement().use { stmt ->
                stmt.execute(sql)
            }
        } finally {
            conn.close()
        }
    }

    private companion object {
        const val HTTP_PORT = 8123
    }
}
