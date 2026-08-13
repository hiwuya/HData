package me.jayer.hdata.jdbc

import java.util.Properties

/**
 * 读写两端共用的连接配置片段。
 *
 * 配置键沿用 Beam YAML 的 `snake_case` 写法，例如 `driver_class` / `connection_properties`。
 *
 * @author wuya
 * @date 2022-07-29
 */
interface JdbcConnectionConfig {
    val url: String
    val user: String
    val password: String

    /** 通常不用填，由 JDBC 的 SPI 自动发现驱动。 */
    val driverClass: String

    /** 透传给 HikariCP 的额外属性，例如 `maximumPoolSize`。 */
    val connectionProperties: Map<String, String>
}

fun JdbcConnectionConfig.validateConnection() {
    require(url.isNotBlank()) { "url 不能为空" }
}

/** 组装成 [com.zaxxer.hikari.HikariConfig] 认识的属性表。 */
fun JdbcConnectionConfig.dataSourceProperties(): Properties = Properties().apply {
    this["jdbcUrl"] = url
    if (user.isNotBlank()) this["dataSource.user"] = user
    if (password.isNotBlank()) this["dataSource.password"] = password
    if (driverClass.isNotBlank()) this["driverClassName"] = driverClass
    connectionProperties.forEach { (key, value) -> this[key] = value }
}
