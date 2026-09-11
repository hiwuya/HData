package me.jayer.hdata.jdbc

import java.util.Properties

/**
 * Connection config fragment shared by the read and write sides.
 *
 * Config keys follow the `snake_case` style of Beam YAML, for example `driver_class` / `connection_properties`.
 *
 * @author wuya
 * @date 2022-07-29
 */
interface JdbcConnectionConfig {
    val url: String
    val user: String
    val password: String

    /** Usually left empty; the driver is discovered automatically through JDBC's SPI. */
    val driverClass: String

    /** Extra properties passed through to HikariCP, for example `maximumPoolSize`. */
    val connectionProperties: Map<String, String>
}

fun JdbcConnectionConfig.validateConnection() {
    require(url.isNotBlank()) { "url must not be blank" }
    require(connectionProperties.keys.none { it.isBlank() }) { "connection_properties must not contain a blank key" }
    val reserved = setOf(
        "jdbcUrl",
        "driverClassName",
        "username",
        "password",
        "dataSource.user",
        "dataSource.password",
    )
    val repeated = connectionProperties.keys.intersect(reserved)
    require(repeated.isEmpty()) {
        "${repeated.sorted()} in connection_properties are managed by the explicit connection config and must not be set again"
    }
}

/** Assembles the property map that [com.zaxxer.hikari.HikariConfig] understands. */
fun JdbcConnectionConfig.dataSourceProperties(): Properties = Properties().apply {
    this["jdbcUrl"] = url
    if (user.isNotBlank()) this["dataSource.user"] = user
    if (password.isNotBlank()) this["dataSource.password"] = password
    if (driverClass.isNotBlank()) this["driverClassName"] = driverClass
    connectionProperties.forEach { (key, value) -> this[key] = value }
}
