package me.jayer.hdata.jdbc

import me.jayer.hdata.core.HData
import me.jayer.hdata.core.spec.PipelineSpecLoader
import me.jayer.hdata.core.spec.SpecMappers
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import java.sql.DriverManager
import kotlin.test.assertEquals

/** Verifies the JDBC source and sink against a real PostgreSQL server. */
@Tag("integration")
class PostgresJdbcIT {

    @Test
    fun `reads from and writes to PostgreSQL`() {
        PostgresContainer().use { database ->
            database.start()
            DriverManager.getConnection(database.jdbcUrl, USER, PASSWORD).use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("CREATE TABLE source_orders (id INTEGER PRIMARY KEY, name VARCHAR(50), paid BOOLEAN)")
                    statement.execute("CREATE TABLE target_orders (id INTEGER PRIMARY KEY, name VARCHAR(50), paid BOOLEAN)")
                    statement.execute("INSERT INTO source_orders VALUES (1, 'alpha', true), (2, 'beta', false)")
                }
            }

            val pipeline = """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromJdbc
                      config:
                        url: "${database.jdbcUrl}"
                        user: "$USER"
                        password: "$PASSWORD"
                        driver_class: "org.postgresql.Driver"
                        tables: ["source_orders"]
                        partition_num: 1
                    - type: WriteToJdbc
                      config:
                        url: "${database.jdbcUrl}"
                        user: "$USER"
                        password: "$PASSWORD"
                        driver_class: "org.postgresql.Driver"
                        table: "target_orders"
                        batch_size: 2
            """.trimIndent()
            HData(PipelineSpecLoader.parse(pipeline, SpecMappers.YAML, "postgres-it"))
                .build(PipelineOptionsFactory.fromArgs("--runner=DirectRunner").create())
                .first.run().waitUntilFinish()

            DriverManager.getConnection(database.jdbcUrl, USER, PASSWORD).use { connection ->
                connection.createStatement().executeQuery("SELECT id, name, paid FROM target_orders ORDER BY id").use { result ->
                    val rows = buildList {
                        while (result.next()) add(listOf(result.getInt(1), result.getString(2), result.getBoolean(3)))
                    }
                    assertEquals(listOf(listOf(1, "alpha", true), listOf(2, "beta", false)), rows)
                }
            }
        }
    }

    private class PostgresContainer : GenericContainer<PostgresContainer>(
        DockerImageName.parse("postgres:17-alpine"),
    ) {
        init {
            withEnv("POSTGRES_DB", DATABASE)
            withEnv("POSTGRES_USER", USER)
            withEnv("POSTGRES_PASSWORD", PASSWORD)
            withExposedPorts(PORT)
        }

        val jdbcUrl: String
            get() = "jdbc:postgresql://$host:${getMappedPort(PORT)}/$DATABASE"
    }

    private companion object {
        const val DATABASE = "hdata"
        const val USER = "hdata"
        const val PASSWORD = "hdata-password"
        const val PORT = 5432
    }
}
