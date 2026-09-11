package me.jayer.hdata.jdbc.internal

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.util.Properties

/**
 * Creation and lifecycle of data sources.
 *
 * Before the refactor there were three ways of doing this: the source opened one inside `expand` and closed it right after use,
 * `JdbcSourceDoFn` created a connection pool **for every element** and destroyed it again, and the splittable DoFn used lazy
 * loading plus `@Teardown` to close. Everything is unified here: DoFns always create in `@Setup` and close in `@Teardown`.
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

    /** One-shot use at graph construction time: for probing metadata, closed as soon as it is done. */
    fun <T> withConnection(properties: Properties, poolName: String, block: (java.sql.Connection) -> T): T =
        create(properties, poolName).use { dataSource -> dataSource.connection.use(block) }

    /**
     * MySQL's `setFetchSize(n)` does not stream — the driver still pulls the entire result set into memory;
     * only `useCursorFetch=true` (or fetchSize=Integer.MIN_VALUE) really goes through a cursor.
     * This is an OOM trap when syncing large tables, so we call it out here up front.
     */
    fun warnIfMySqlWithoutCursor(jdbcUrl: String, fetchSize: Int) {
        val url = jdbcUrl.lowercase()
        if (!url.startsWith("jdbc:mysql:") && !url.startsWith("jdbc:mariadb:")) return
        if (url.contains("usecursorfetch=true")) return
        LOGGER.warn(
            "MySQL fetch_size={} has no effect: the driver reads the whole result set into memory." +
                "For large tables add useCursorFetch=true to the url, otherwise it may OOM.",
            fetchSize,
        )
    }
}
