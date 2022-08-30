package io.jayer.hdata.jdbc

import io.jayer.hdata.core.spi.StructuredIOProvider
import io.jayer.hdata.core.spi.StructuredSink
import io.jayer.hdata.core.spi.StructuredSource
import io.jayer.hdata.core.util.ObjectMappers
import java.util.*

/**
 * @author wuya
 * @date 2022-08-05
 */
class JdbcStructuredIOProvider : StructuredIOProvider {

    override fun identifier(): String = "jdbc"

    private fun updateDataSourceConfig(dataSourceConfig: Properties, config: Map<String, Any>) {
        val jdbcUrl = requireNotNull(config["url"]) { "Config \"url\" requires not null" }
        dataSourceConfig["jdbcUrl"] = jdbcUrl

        val user = requireNotNull(config["user"]) { "Config \"user\" requires not null" }
        dataSourceConfig["dataSource.user"] = user

        val password = requireNotNull(config["password"]) { "Config \"password\" requires not null" }
        dataSourceConfig["dataSource.password"] = password
    }

    override fun createSource(config: Map<String, Any>): StructuredSource {
        val jdbcSourceDescriptor = ObjectMappers.getDefault().convertValue(config, JdbcSourceDescriptor::class.java)
        updateDataSourceConfig(jdbcSourceDescriptor.dataSourceConfig, config)
        jdbcSourceDescriptor.validate()
        return JdbcStructuredSource(jdbcSourceDescriptor)
    }

    override fun createSink(config: Map<String, Any>): StructuredSink {
        val jdbcSinkDescriptor = ObjectMappers.getDefault().convertValue(config, JdbcSinkDescriptor::class.java)
        updateDataSourceConfig(jdbcSinkDescriptor.dataSourceConfig, config)
        jdbcSinkDescriptor.validate()
        return JdbcStructuredSink(jdbcSinkDescriptor)
    }
}