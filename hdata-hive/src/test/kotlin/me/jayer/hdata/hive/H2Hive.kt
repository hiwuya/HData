package me.jayer.hdata.hive

import java.sql.Connection
import java.sql.DriverManager

/**
 * 用 H2 内存库顶替 HiveServer2 做端到端测试。
 *
 * `ReadFromHive` / `WriteToHive` 走的是纯 JDBC：连接、结果集元数据、fetch size、批量 INSERT，
 * 这条链路 H2 和 HiveServer2 是一样的。真正 Hive 特有的部分只有 `SHOW PARTITIONS` 的输出格式，
 * 那部分由 [HivePartitionsTest] 单独覆盖，这里用显式 `partitions` 配置绕开。
 *
 * URL 上的 `DB_CLOSE_DELAY=-1` 不能少：构图阶段会开一次连接探 schema 再关掉，
 * 没有这个参数内存库当场就没了。
 *
 * @author wuya
 */
class H2Hive(name: String) : AutoCloseable {

    val url: String = "jdbc:h2:mem:$name;DB_CLOSE_DELAY=-1;MODE=MySQL"

    private val keepAlive: Connection = DriverManager.getConnection(url)

    fun execute(vararg sql: String) {
        keepAlive.createStatement().use { st -> sql.forEach { st.execute(it) } }
    }

    fun <T> useConnection(block: (Connection) -> T): T =
        DriverManager.getConnection(url).use(block)

    fun queryColumn(sql: String, column: String): List<Any?> = useConnection { connection ->
        connection.createStatement().use { st ->
            st.executeQuery(sql).use { rs ->
                generateSequence { if (rs.next()) rs.getObject(column) else null }.toList()
            }
        }
    }

    fun count(table: String): Long = useConnection { connection ->
        connection.createStatement().use { st ->
            st.executeQuery("SELECT COUNT(*) FROM $table").use { rs ->
                rs.next()
                rs.getLong(1)
            }
        }
    }

    override fun close() {
        runCatching { keepAlive.createStatement().use { it.execute("DROP ALL OBJECTS") } }
        runCatching { keepAlive.close() }
    }

    companion object {
        fun named(name: String): H2Hive = H2Hive("$name-${System.nanoTime()}")
    }
}
