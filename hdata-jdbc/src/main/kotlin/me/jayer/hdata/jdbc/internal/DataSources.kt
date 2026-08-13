package me.jayer.hdata.jdbc.internal

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.util.Properties

/**
 * 数据源的创建与生命周期。
 *
 * 重构前有三套写法：source 在 `expand` 里开一个用完就关，`JdbcSourceDoFn` **每来一个元素**就新建
 * 一个连接池再销毁，splittable DoFn 则是懒加载 + `@Teardown` 关闭。这里统一成一处，
 * DoFn 一律在 `@Setup` 建、`@Teardown` 关。
 *
 * @author wuya
 * @date 2022-08-04
 */
object DataSources {

    private val LOGGER = LoggerFactory.getLogger(DataSources::class.java)

    fun create(properties: Properties, poolName: String): HikariDataSource {
        val config = HikariConfig(properties)
        config.poolName = poolName
        return HikariDataSource(config)
    }

    /** 构图期的一次性使用：探测元数据用，用完即关。 */
    fun <T> withConnection(properties: Properties, poolName: String, block: (java.sql.Connection) -> T): T =
        create(properties, poolName).use { dataSource -> dataSource.connection.use(block) }

    /**
     * MySQL 的 `setFetchSize(n)` 不会流式读取——驱动仍会把整个结果集拉进内存，
     * 只有 `useCursorFetch=true`（或 fetchSize=Integer.MIN_VALUE）才真正走游标。
     * 大表同步时这是个 OOM 陷阱，这里提前提醒一句。
     */
    fun warnIfMySqlWithoutCursor(jdbcUrl: String, fetchSize: Int) {
        val url = jdbcUrl.lowercase()
        if (!url.startsWith("jdbc:mysql:") && !url.startsWith("jdbc:mariadb:")) return
        if (url.contains("usecursorfetch=true")) return
        LOGGER.warn(
            "MySQL 下 fetch_size={} 不会生效：驱动会把整个结果集读进内存。" +
                "大表请在 url 上加 useCursorFetch=true，否则可能 OOM。",
            fetchSize,
        )
    }
}
