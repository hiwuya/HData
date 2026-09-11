package me.jayer.hdata.redis

import me.jayer.hdata.redis.internal.RedisNodeConfig
import java.io.Serializable

/**
 * Configuration for `ReadFromRedis`.
 *
 * Modes: `scan` (default) scans keys matching `key_pattern`; `keys` reads explicit keys; `stream` uses XRANGE.
 *
 * The output schema is fixed by mode; connection fields are defined by [RedisNodeConfig].
 *
 * @author wuya
 */
data class RedisReadConfig(
    override val host: String = "localhost",
    override val port: Int = 6379,
    override val password: String = "",
    override val database: Int = 0,
    override val ssl: Boolean = false,
    override val timeoutMs: Int = 5000,

    /** `scan` (default), `keys`, or `stream`. */
    val mode: String = MODE_SCAN,
    /** SCAN match pattern for `mode=scan`. */
    val keyPattern: String = "*",
    /** Keys to read for `mode=keys`. */
    val keys: List<String> = emptyList(),
    /** Stream name for `mode=stream`. */
    val stream: String = "",
    /** Start and end entry IDs for `mode=stream`; `-` and `+` select boundaries. */
    val startId: String = "-",
    val endId: String = "+",
) : RedisNodeConfig, Serializable {

    fun validate() {
        validateNode()
        require(mode in MODES) { "invalid mode: $mode; allowed values: ${MODES.joinToString()}" }
        when (mode) {
            MODE_KEYS -> {
                require(keys.isNotEmpty()) { "mode=keys requires keys" }
                require(keys.none { it.isBlank() }) { "keys must not contain a blank key" }
                require(stream.isBlank() && startId == "-" && endId == "+") {
                    "mode=keys does not use stream/start_id/end_id; remove them from the configuration"
                }
            }
            MODE_STREAM -> {
                require(stream.isNotBlank()) { "mode=stream requires stream" }
                require(keys.isEmpty() && keyPattern == "*") {
                    "mode=stream does not use keys/key_pattern; remove them from the configuration"
                }
                // Parse IDs during graph construction so invalid input fails before the job runs.
                val start = me.jayer.hdata.redis.transform.parseStreamId(startId, org.redisson.api.StreamMessageId.MIN)
                val end = me.jayer.hdata.redis.transform.parseStreamId(endId, org.redisson.api.StreamMessageId.MAX)
                require(me.jayer.hdata.redis.transform.compareStreamIds(start, end) <= 0) {
                    "start_id must be <= end_id; received start_id[$startId] > end_id[$endId]"
                }
            }
            MODE_SCAN -> {
                require(keyPattern.isNotBlank()) { "mode=scan requires key_pattern" }
                require(keys.isEmpty() && stream.isBlank() && startId == "-" && endId == "+") {
                    "mode=scan does not use keys/stream/start_id/end_id; remove them from the configuration"
                }
            }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1

        const val MODE_SCAN = "scan"
        const val MODE_KEYS = "keys"
        const val MODE_STREAM = "stream"
        val MODES = listOf(MODE_SCAN, MODE_KEYS, MODE_STREAM)
    }
}
