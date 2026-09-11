package me.jayer.hdata.cassandra.internal

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.CqlSessionBuilder
import java.net.InetSocketAddress

/**
 * Creates DataStax CqlSession instances from configuration parameters.
 *
 * @author wuya
 */
internal object CassandraSessions {

    fun newSession(
        endpoints: List<String>,
        keyspace: String,
        connectTimeoutMs: Int,
        requestTimeoutMs: Int,
    ): CqlSession {
        val builder = CqlSession.builder()
        for (endpoint in endpoints) {
            val host = endpoint.substringBeforeLast(":")
            val port = endpoint.substringAfterLast(":").toInt()
            builder.addContactPoint(InetSocketAddress(host, port))
        }
        builder.withLocalDatacenter("datacenter1") // Default for single-node / testcontainer.
        if (keyspace.isNotBlank()) {
            builder.withKeyspace(keyspace)
        }
        return builder.build()
    }
}
