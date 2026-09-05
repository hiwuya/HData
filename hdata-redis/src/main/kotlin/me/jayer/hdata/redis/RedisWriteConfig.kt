package me.jayer.hdata.redis

import me.jayer.hdata.redis.internal.RedisNodeConfig
import java.io.Serializable

/**
 * `WriteToRedis` 的配置。
 *
 * 写入模式（每个模式对应的行字段名可用 `key_field` / `value_field` / `hash_field` 重命名）：
 * - `set`(默认)：`SET key value`（可带 `ttl_seconds` 过期）；
 * - `lpush` / `rpush`：把 `value` 推入以 `key` 为名的 list 头部 / 尾部；
 * - `sadd`：把 `value` 加入以 `key` 为名的 set；
 * - `hset`：把 `value` 写入以 `key` 为名的 hash 的 `field` 字段。
 *
 * 连接字段见 [RedisNodeConfig]。
 *
 * @author wuya
 */
data class RedisWriteConfig(
    override val host: String = "localhost",
    override val port: Int = 6379,
    override val password: String = "",
    override val database: Int = 0,
    override val ssl: Boolean = false,
    override val timeoutMs: Int = 5000,

    /** `set`(默认) / `lpush` / `rpush` / `sadd` / `hset`。 */
    val mode: String = MODE_SET,
    /** 行里作为 Redis key 的字段（默认 `key`）。 */
    val keyField: String = "key",
    /** 行里作为 Redis value 的字段（默认 `value`）。 */
    val valueField: String = "value",
    /** `mode=hset` 时作为 hash 字段名的行字段（默认 `field`）。 */
    val hashField: String = "field",
    /** 过期秒数，`set` 下用 SETEX，其余用 EXPIRE；不填表示不过期。 */
    val ttlSeconds: Long? = null,
) : RedisNodeConfig, Serializable {

    fun validate() {
        validateNode()
        require(mode in MODES) { "mode 取值非法: $mode，可选 ${MODES.joinToString()}" }
        require(keyField.isNotBlank()) { "key_field 不能为空" }
        require(valueField.isNotBlank()) { "value_field 不能为空" }
        if (mode == MODE_HSET) {
            require(hashField.isNotBlank()) { "mode=hset 需要 hash_field" }
        } else {
            require(hashField == "field") { "mode=$mode 不使用 hash_field，请从配置中移除" }
        }
        require(ttlSeconds == null || ttlSeconds > 0) { "ttl_seconds 必须大于 0" }
    }

    companion object {
        private const val serialVersionUID: Long = 1

        const val MODE_SET = "set"
        const val MODE_LPUSH = "lpush"
        const val MODE_RPUSH = "rpush"
        const val MODE_SADD = "sadd"
        const val MODE_HSET = "hset"
        val MODES = listOf(MODE_SET, MODE_LPUSH, MODE_RPUSH, MODE_SADD, MODE_HSET)
    }
}
