package me.jayer.hdata.cassandra.internal

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import java.net.InetSocketAddress
import java.time.Duration

/**
 * Creates DataStax CqlSession instances from configuration parameters.
 *
 * @author wuya
 */
internal object CassandraSessions {

    fun newSession(
        endpoints: List<String>,
        keyspace: String,
        datacenter: String,
        connectTimeoutMs: Int,
        requestTimeoutMs: Int,
    ): CqlSession {
        val builder = CqlSession.builder()
        for (endpoint in endpoints) {
            val host = endpoint.substringBeforeLast(":")
            val port = endpoint.substringAfterLast(":").toInt()
            builder.addContactPoint(InetSocketAddress(host, port))
        }
        builder.withLocalDatacenter(datacenter)
        if (keyspace.isNotBlank()) {
            builder.withKeyspace(keyspace)
        }

        // Apply timeouts via the programmatic driver config loader.
        val configBuilder = DriverConfigLoader.programmaticBuilder()
        if (connectTimeoutMs > 0) {
            configBuilder.withDuration(
                com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT,
                Duration.ofMillis(connectTimeoutMs.toLong()),
            )
        }
        if (requestTimeoutMs > 0) {
            configBuilder.withDuration(
                com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_TIMEOUT,
                Duration.ofMillis(requestTimeoutMs.toLong()),
            )
        }
        builder.withConfigLoader(configBuilder.build())

        return builder.build()
    }
}
