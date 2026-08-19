package me.jayer.hdata.redis

import me.jayer.hdata.redis.internal.RedisNodeConfig
import java.io.Serializable

/**
 * `ReadFromRedis` 的配置。
 *
 * 读取模式：
 * - `scan`(默认)：用 SCAN 遍历 `key_pattern`（默认 `*`）匹配到的 key，每行输出 `key` / `value`；
 * - `keys`：只读取显式给定的 `keys` 列表；
 * - `stream`：用 XRANGE 读取一个 Redis Stream（`stream`）里 `-` 到 `+` 的全部条目，
 *   每个条目的每个字段输出一行（`id` / `field` / `value`），便于直接接关系型下游。
 *
 * 读出行的 schema 由模式固定，不要求用户在 pipeline 里声明 `schema_fields`。
 *
 * 连接字段见 [RedisNodeConfig]。
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

    /** `scan`(默认) / `keys` / `stream`。 */
    val mode: String = MODE_SCAN,
    /** `mode=scan` 用：SCAN 的匹配模式。 */
    val keyPattern: String = "*",
    /** `mode=keys` 用：要读取的 key 列表。 */
    val keys: List<String> = emptyList(),
    /** `mode=stream` 用：要读取的 stream 名。 */
    val stream: String = "",
    /** `mode=stream` 用：起始/结束 entry id，`-` / `+` 表示首尾。 */
    val startId: String = "-",
    val endId: String = "+",
) : RedisNodeConfig, Serializable {

    fun validate() {
        validateNode()
        require(mode in MODES) { "mode 取值非法: $mode，可选 ${MODES.joinToString()}" }
        when (mode) {
            MODE_KEYS -> {
                require(keys.isNotEmpty()) { "mode=keys 需要 keys" }
                require(keys.none { it.isBlank() }) { "keys 不能包含空 key" }
                require(stream.isBlank() && startId == "-" && endId == "+") {
                    "mode=keys 不使用 stream/start_id/end_id，请从配置中移除"
                }
            }
            MODE_STREAM -> {
                require(stream.isNotBlank()) { "mode=stream 需要 stream" }
                require(keys.isEmpty() && keyPattern == "*") {
                    "mode=stream 不使用 keys/key_pattern，请从配置中移除"
                }
                // 构图阶段就把 entry id 解析一遍：写错了当场报错，而不是等作业跑起来才发现
                me.jayer.hdata.redis.transform.parseStreamId(startId, org.redisson.api.StreamMessageId.MIN)
                me.jayer.hdata.redis.transform.parseStreamId(endId, org.redisson.api.StreamMessageId.MAX)
            }
            MODE_SCAN -> {
                require(keyPattern.isNotBlank()) { "mode=scan 需要 key_pattern" }
                require(keys.isEmpty() && stream.isBlank() && startId == "-" && endId == "+") {
                    "mode=scan 不使用 keys/stream/start_id/end_id，请从配置中移除"
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
