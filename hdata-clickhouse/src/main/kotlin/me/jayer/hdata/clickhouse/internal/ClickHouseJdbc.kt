package me.jayer.hdata.clickhouse.internal

/**
 * Shared utilities for ClickHouse JDBC connections.
 *
 * @author wuya
 */
internal object ClickHouseJdbc {

    /**
     * Builds a ClickHouse JDBC URL from an HTTP endpoint, database name, and optional timeout
     * parameters. The endpoint may be an HTTP URL (`http://host:port`) or a bare `host:port`
     * pair; it is normalised to `jdbc:clickhouse://host:port/database`.
     */
    fun buildJdbcUrl(
        endpoint: String,
        database: String,
        connectTimeoutMs: Int = 0,
        socketTimeoutMs: Int = 0,
    ): String {
        val trimmed = endpoint.trimEnd('/')
        val base = if (trimmed.startsWith("http://", ignoreCase = true) ||
            trimmed.startsWith("https://", ignoreCase = true)
        ) {
            val host = try {
                java.net.URI(trimmed).host ?: "localhost"
            } catch (_: Exception) {
                "localhost"
            }
            val port = try {
                java.net.URI(trimmed).port.takeIf { it > 0 } ?: 8123
            } catch (_: Exception) {
                8123
            }
            "jdbc:clickhouse://$host:$port"
        } else {
            trimmed
        }

        val params = mutableListOf<String>()
        if (connectTimeoutMs > 0) params.add("connect_timeout=$connectTimeoutMs")
        if (socketTimeoutMs > 0) params.add("socket_timeout=$socketTimeoutMs")

        val url = "$base/$database"
        return if (params.isNotEmpty()) "$url?${params.joinToString("&")}" else url
    }
}
