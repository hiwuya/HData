package me.jayer.hdata.redis.internal

import org.redisson.Redisson
import org.redisson.api.RedissonClient
import org.redisson.config.Config
import java.io.Serializable

/**
 * Creates a Redis connection through [RedissonClient]. Read and write configs share connection fields;
 * this function centralizes SSL, authentication, and database selection.
 *
 * Each DoFn creates a client in `@Setup` and closes it in `@Teardown`; the client is excluded from constructor
 * state because RedissonClient is not serializable.
 */
fun newRedisson(
    host: String,
    port: Int,
    password: String,
    database: Int,
    ssl: Boolean,
    timeoutMs: Int,
): RedissonClient {
    val config = Config()
    val scheme = if (ssl) "rediss://" else "redis://"
    val single = config.useSingleServer()
    single.address = "$scheme$host:$port"
    single.database = database
    single.timeout = timeoutMs
    single.connectTimeout = timeoutMs
    if (password.isNotBlank()) single.password = password
    return Redisson.create(config)
}

/** Connection fields shared by read and write configuration. */
interface RedisNodeConfig : Serializable {
    val host: String
    val port: Int
    val password: String
    val database: Int
    val ssl: Boolean
    val timeoutMs: Int

    fun validateNode() {
        require(host.isNotBlank()) { "host must not be blank" }
        require(port in 1..65535) { "port must be between 1 and 65535" }
        require(database >= 0) { "database must not be negative" }
        require(timeoutMs > 0) { "timeout_ms must be greater than 0" }
    }
}
