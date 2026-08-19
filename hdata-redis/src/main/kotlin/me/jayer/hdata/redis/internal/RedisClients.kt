package me.jayer.hdata.redis.internal

import org.redisson.Redisson
import org.redisson.api.RedissonClient
import org.redisson.config.Config
import java.io.Serializable

/**
 * 建一个到 Redis 的连接（Redisson 的 [RedissonClient]）。连接字段直接平铺在读写配置里
 * （与 Kafka 的 `bootstrap_servers` 风格一致），这里集中处理 ssl / 鉴权 / 库号。
 *
 * 每个 DoFn 实例在 `@Setup` 里建一个 [RedissonClient]、在 `@Teardown` 关掉，
 * 连接本身不进构造函数，避免 RedissonClient 不可序列化导致整个 DoFn 无法下发。
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

/** 读写配置共用的连接字段。 */
interface RedisNodeConfig : Serializable {
    val host: String
    val port: Int
    val password: String
    val database: Int
    val ssl: Boolean
    val timeoutMs: Int

    fun validateNode() {
        require(host.isNotBlank()) { "host 不能为空" }
        require(port in 1..65535) { "port 必须在 1..65535 之间" }
        require(database >= 0) { "database 不能为负" }
        require(timeoutMs > 0) { "timeout_ms 必须大于 0" }
    }
}
