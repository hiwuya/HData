package me.jayer.hdata.redis

import me.jayer.hdata.redis.internal.RedisNodeConfig
import java.io.Serializable

/**
 * Configuration for `WriteToRedis`.
 *
 * Supports set, list, set-member, and hash-field writes. Connection fields are defined by [RedisNodeConfig].
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

    /** `set` (default), `lpush`, `rpush`, `sadd`, or `hset`. */
    val mode: String = MODE_SET,
    /** Input field used as the Redis key, defaulting to `key`. */
    val keyField: String = "key",
    /** Input field used as the Redis value, defaulting to `value`. */
    val valueField: String = "value",
    /** Input field used as a hash field name for `mode=hset`, defaulting to `field`. */
    val hashField: String = "field",
    /** Expiry in seconds; omitted means no expiry. */
    val ttlSeconds: Long? = null,
) : RedisNodeConfig, Serializable {

    fun validate() {
        validateNode()
        require(mode in MODES) { "invalid mode: $mode; allowed values: ${MODES.joinToString()}" }
        require(keyField.isNotBlank()) { "key_field must not be blank" }
        require(valueField.isNotBlank()) { "value_field must not be blank" }
        if (mode == MODE_HSET) {
            require(hashField.isNotBlank()) { "mode=hset requires hash_field" }
        } else {
            require(hashField == "field") { "mode=$mode does not use hash_field; remove it from the configuration" }
        }
        require(ttlSeconds == null || ttlSeconds > 0) { "ttl_seconds must be > 0" }
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
