package me.jayer.hdata.kafka

import java.io.Serializable

/**
 * `ReadFromKafka` 的配置。配置键对齐 Flink Kafka connector：
 * `scan.startup.mode` 支持 `earliest-offset` / `latest-offset` / `group-offsets` /
 * `specific-offsets` / `timestamp`，[scanStartupSpecificOffsets] 与 [scanStartupTimestampMillis] 配套使用。
 *
 * ```yaml
 * - type: ReadFromKafka
 *   config:
 *     bootstrap_servers: "localhost:9092"
 *     topics: ["orders"]
 *     scan_startup_mode: earliest-offset
 *     offset_split_size: 100000
 * ```
 *
 * 读出的行固定带 schema：`key` / `value`(STRING, 可空)、`topic` / `partition` / `offset` / `timestamp`。
 */
data class KafkaReadConfig(
    val bootstrapServers: String = "",
    val topics: List<String> = emptyList(),
    val groupId: String = "",
    /** 透传给 KafkaConsumer 的额外属性，例如 `fetch.max.wait.ms`。 */
    val consumerConfig: Map<String, String> = emptyMap(),
    /** `earliest-offset`(默认) / `latest-offset` / `group-offsets` / `specific-offsets` / `timestamp`。 */
    val scanStartupMode: String = "earliest-offset",
    /** `specific-offsets` 模式用：`"topic:partition" -> offset`。 */
    val scanStartupSpecificOffsets: Map<String, Long> = emptyMap(),
    /** `timestamp` 模式用：起始毫秒时间戳。 */
    val scanStartupTimestampMillis: Long? = null,
    /** 每个切分的最大消息数，决定 Splittable DoFn 把分区范围切成几段。 */
    val offsetSplitSize: Long = 100_000,
    val keyFormat: String = "string",
    val valueFormat: String = "string",
) : Serializable {

    fun validate() {
        require(bootstrapServers.isNotBlank()) { "bootstrap_servers 不能为空" }
        require(topics.isNotEmpty()) { "topics 至少要填一个" }
        require(offsetSplitSize > 0) { "offset_split_size 必须 > 0" }
        require(
            scanStartupMode in setOf(
                "earliest-offset", "latest-offset", "group-offsets",
                "specific-offsets", "timestamp",
            )
        ) { "scan_startup_mode 取值非法: $scanStartupMode" }
    }
}
