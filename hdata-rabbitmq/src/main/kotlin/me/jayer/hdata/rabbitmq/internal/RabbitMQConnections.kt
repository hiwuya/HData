package me.jayer.hdata.rabbitmq.internal

import com.rabbitmq.client.ConnectionFactory

/**
 * Creates RabbitMQ connections and channels with the given parameters.
 *
 * @author wuya
 */
internal object RabbitMQConnections {

    fun newFactory(
        host: String,
        port: Int,
        virtualHost: String,
        username: String,
        password: String,
    ): ConnectionFactory = ConnectionFactory().apply {
        this.host = host
        this.port = port
        this.virtualHost = virtualHost
        this.username = username
        this.password = password
        // Allow a reasonable timeout for connection and channel establishment.
        this.connectionTimeout = 10_000
        this.handshakeTimeout = 10_000
        this.channelRpcTimeout = 10_000
    }
}
