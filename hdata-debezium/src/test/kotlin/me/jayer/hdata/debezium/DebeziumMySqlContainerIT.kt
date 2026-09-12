package me.jayer.hdata.debezium

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.node.ObjectNode
import java.nio.file.Files
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Exercises the Debezium embedded engine against a real MySQL binlog. This is the one Debezium test that a mocked
 * `SourceRecord` cannot stand in for: it proves the config actually produces a working `database.server.id` /
 * `topic.prefix` / offset-storage combination that a live MySQL connector accepts.
 */
@Tag("integration")
class DebeziumMySqlContainerIT {
    @Test
    fun `captures an initial snapshot from a real MySQL binlog`() {
        MySqlContainer().use { mysql ->
            mysql.start()
            DriverManager.getConnection(mysql.jdbcUrl, USER, PASSWORD).use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50))")
                    statement.execute("INSERT INTO customers VALUES (1, 'alpha'), (2, 'beta')")
                }
            }

            val yaml = """
                connector: mysql
                host: "${mysql.host}"
                port: ${mysql.getMappedPort(PORT)}
                user: "$USER"
                password: "$PASSWORD"
                database: "$DATABASE"
                table_include: "$DATABASE.customers"
                snapshot_mode: "initial"
                max_records: 2
            """.trimIndent()
            val config = TransformConfig("ReadFromDebezium", SpecMappers.YAML.readTree(yaml) as ObjectNode)

            val pipeline = Pipeline.create()
            val output = PCollectionRowTuple.empty(pipeline)
                .apply(DebeziumReadProvider().from(config))
                .get(Tags.MAIN_OUTPUT)
            PAssert.that(output).satisfies(
                SerializableFunction<Iterable<Row>, Void?> { rows ->
                    val list = rows.toList()
                    assertEquals(2, list.size)
                    val mapper = ObjectMapper()
                    val customers = list.map { row ->
                        assertEquals("r", row.getString("op"))
                        val after = requireNotNull(row.getString("after")) { "snapshot row is missing after" }
                        val fields = mapper.readTree(after)
                        fields.get("id").asInt() to fields.get("name").asString()
                    }.toSet()
                    assertEquals(setOf(1 to "alpha", 2 to "beta"), customers)
                    null
                },
            )
            pipeline.run().waitUntilFinish()
        }
    }

    /**
     * The synthetic-connector version of this scenario ([DebeziumRecoveryTest]) proves the offset-commit and
     * offset-read code path works; this proves it against a real MySQL binlog and real Debezium snapshot
     * completion tracking, which is the gap `DebeziumRecoveryTest` explicitly does not close (see
     * docs/MATURITY_ASSESSMENT.md, critical gap #1).
     */
    @Test
    fun `a restart resumes streaming after a completed snapshot, without re-snapshotting`() {
        MySqlContainer().use { mysql ->
            mysql.start()
            DriverManager.getConnection(mysql.jdbcUrl, USER, PASSWORD).use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("CREATE TABLE orders (id INT PRIMARY KEY, item VARCHAR(50))")
                    statement.execute("INSERT INTO orders VALUES (1, 'pen'), (2, 'pencil')")
                }
            }

            val offsetFile = Files.createTempFile("it-offset", ".dat").toString()
            val schemaHistoryFile = Files.createTempFile("it-schema-history", ".dat").toString()

            fun configYaml(maxRecords: Int) = """
                connector: mysql
                host: "${mysql.host}"
                port: ${mysql.getMappedPort(PORT)}
                user: "$USER"
                password: "$PASSWORD"
                database: "$DATABASE"
                table_include: "$DATABASE.orders"
                snapshot_mode: "initial"
                offset_file: "$offsetFile"
                schema_history_file: "$schemaHistoryFile"
                lease_timeout_ms: 50
                max_records: $maxRecords
            """.trimIndent()

            // First run: captures the initial snapshot (the two seed rows) and persists offset + schema history.
            val firstConfig = TransformConfig("ReadFromDebezium", SpecMappers.YAML.readTree(configYaml(2)) as ObjectNode)
            val firstPipeline = Pipeline.create()
            val firstOutput = PCollectionRowTuple.empty(firstPipeline)
                .apply(DebeziumReadProvider().from(firstConfig))
                .get(Tags.MAIN_OUTPUT)
            PAssert.that(firstOutput).satisfies(
                SerializableFunction<Iterable<Row>, Void?> { rows ->
                    val list = rows.toList()
                    assertEquals(2, list.size)
                    assertTrue(list.all { it.getString("op") == "r" }, "the first run must only see the initial snapshot")
                    null
                },
            )
            firstPipeline.run().waitUntilFinish()

            // Insert new rows only after the snapshot has completed, so they can only be observed via streaming.
            DriverManager.getConnection(mysql.jdbcUrl, USER, PASSWORD).use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("INSERT INTO orders VALUES (3, 'eraser'), (4, 'ruler')")
                }
            }
            // Lets the first run's lease (lease_timeout_ms=50) go stale regardless of whether its @Teardown
            // ran — the whole point of OffsetLease is that a restart must not depend on that callback firing.
            Thread.sleep(200)

            // Second run, same offset_file/schema_history_file: must resume streaming from the persisted binlog
            // position, seeing only the two new inserts — never re-snapshotting the original two rows.
            val secondConfig = TransformConfig("ReadFromDebezium", SpecMappers.YAML.readTree(configYaml(2)) as ObjectNode)
            val secondPipeline = Pipeline.create()
            val secondOutput = PCollectionRowTuple.empty(secondPipeline)
                .apply(DebeziumReadProvider().from(secondConfig))
                .get(Tags.MAIN_OUTPUT)
            val mapper = ObjectMapper()
            PAssert.that(secondOutput).satisfies(
                SerializableFunction<Iterable<Row>, Void?> { rows ->
                    val list = rows.toList()
                    assertEquals(
                        2,
                        list.size,
                        "a restart must resume streaming, not replay the snapshot or lose the new inserts",
                    )
                    val ids = list.map { row ->
                        assertEquals("c", row.getString("op"), "new rows arrive as binlog insert events, not a re-snapshot")
                        val after = requireNotNull(row.getString("after"))
                        mapper.readTree(after).get("id").asInt()
                    }.toSet()
                    assertEquals(setOf(3, 4), ids, "must see exactly the rows inserted after the first run's snapshot")
                    null
                },
            )
            secondPipeline.run().waitUntilFinish()
        }
    }

    private class MySqlContainer : GenericContainer<MySqlContainer>(DockerImageName.parse("mysql:8.0")) {
        init {
            withEnv("MYSQL_ROOT_PASSWORD", PASSWORD)
            withEnv("MYSQL_DATABASE", DATABASE)
            withCommand("--log-bin=mysql-bin", "--binlog-format=ROW", "--server-id=1", "--gtid-mode=ON", "--enforce-gtid-consistency=ON")
            withExposedPorts(PORT)
            // "ready for connections" also matches the X Plugin banner, and the entrypoint restarts mysqld once
            // after first-time init, so anchor on the specific "mysqld: ready for connections" line, twice (the
            // temporary init server, then the real one) — matching it loosely would proceed after the first restart.
            waitingFor(Wait.forLogMessage(".*mysqld: ready for connections.*\\n", 2))
        }

        val jdbcUrl: String
            get() = "jdbc:mysql://$host:${getMappedPort(PORT)}/$DATABASE"
    }

    private companion object {
        const val PORT = 3306
        const val DATABASE = "inventory"
        const val USER = "root"
        const val PASSWORD = "root"
    }
}
