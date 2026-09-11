package me.jayer.hdata.jdbc

import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.atomic.AtomicInteger

/**
 * One independent H2 in-memory database per test.
 *
 * `DB_CLOSE_DELAY=-1` keeps the database alive after the last connection is closed — JdbcSource opens a connection at graph
 * construction time to infer the schema and closes it again, and without this parameter the database would be gone.
 *
 * @author wuya
 * @date 2022-08-30
 */
class H2Database(name: String) : AutoCloseable {

    val url: String = "jdbc:h2:mem:$name;DB_CLOSE_DELAY=-1"

    /** Holds one connection for the whole time so the in-memory database survives the entire test. */
    private val keepAlive: Connection = DriverManager.getConnection(url, USER, PASSWORD)

    fun <T> useConnection(block: (Connection) -> T): T =
        DriverManager.getConnection(url, USER, PASSWORD).use(block)

    fun execute(vararg sql: String) = useConnection { connection ->
        connection.createStatement().use { statement -> sql.forEach { statement.execute(it) } }
    }

    /** Reads one column, handy for asserting the written result. */
    fun <T> queryColumn(sql: String, column: Int = 1): List<T> = useConnection { connection ->
        connection.createStatement().use { statement ->
            statement.executeQuery(sql).use { rs ->
                buildList {
                    @Suppress("UNCHECKED_CAST")
                    while (rs.next()) add(rs.getObject(column) as T)
                }
            }
        }
    }

    fun count(table: String): Long = queryColumn<Number>("SELECT count(*) FROM $table").first().toLong()

    override fun close() {
        keepAlive.createStatement().use { it.execute("SHUTDOWN") }
        keepAlive.close()
    }

    companion object {
        const val USER = "sa"
        const val PASSWORD = ""

        private val SEQUENCE = AtomicInteger()

        /** Uses the call site name plus a sequence number so different tests do not share the same in-memory database. */
        fun named(prefix: String): H2Database = H2Database("${prefix}_${SEQUENCE.incrementAndGet()}")
    }
}
