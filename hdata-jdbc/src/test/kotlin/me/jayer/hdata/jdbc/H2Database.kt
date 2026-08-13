package me.jayer.hdata.jdbc

import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.atomic.AtomicInteger

/**
 * 每个测试一个独立的 H2 内存库。
 *
 * `DB_CLOSE_DELAY=-1` 让库在最后一个连接断开后依然存活——JdbcSource 在构图阶段推断 schema 时
 * 会开一次连接又关掉，没有这个参数库就没了。
 *
 * @author wuya
 * @date 2022-08-30
 */
class H2Database(name: String) : AutoCloseable {

    val url: String = "jdbc:h2:mem:$name;DB_CLOSE_DELAY=-1"

    /** 全程持有一个连接，确保内存库在整个测试期间存活。 */
    private val keepAlive: Connection = DriverManager.getConnection(url, USER, PASSWORD)

    fun <T> useConnection(block: (Connection) -> T): T =
        DriverManager.getConnection(url, USER, PASSWORD).use(block)

    fun execute(vararg sql: String) = useConnection { connection ->
        connection.createStatement().use { statement -> sql.forEach { statement.execute(it) } }
    }

    /** 查一列出来，方便断言写入结果。 */
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

        /** 用调用点的名字加序号，避免不同测试共用同一个内存库。 */
        fun named(prefix: String): H2Database = H2Database("${prefix}_${SEQUENCE.incrementAndGet()}")
    }
}
