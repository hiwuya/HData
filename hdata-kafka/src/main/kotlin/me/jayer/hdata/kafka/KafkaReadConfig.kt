package me.jayer.hdata.kafka

import java.io.Serializable

/**
 * `ReadFromKafka` 的配置，键名对齐 Flink Kafka connector 的表参数。
 *
 * ```yaml
 * - type: ReadFromKafka
 *   config:
 *     bootstrap_servers: "localhost:9092"
 *     topics: ["orders"]
 *     scan_startup_mode: earliest-offset
 *     scan_bounded_mode: latest-offset
 * ```
 *
 * 与 Flink 的两处**有意的不同**：
 *  1. [scanBoundedMode] 默认是 `latest-offset` 而不是 Flink 的 `unbounded`。HData 主要用于批量同步，
 *     默认跑成一个永不结束的流作业会让人措手不及；要流式消费显式写 `scan_bounded_mode: unbounded`。
 *  2. `specific-offsets` 的偏移量用 `"topic:partition" -> offset` 的形式给，而不是 Flink 的
 *     `partition:0,offset:42` 字符串，因为这里允许一次读多个 topic。
 *
 * 读出的行 schema 见 [KafkaFormats.readSchema]。
 *
 * @author wuya
 */
data class KafkaReadConfig(
    val bootstrapServers: String = "",
    /** 要读的 topic 列表，与 [topicPattern] 二选一。 */
    val topics: List<String> = emptyList(),
    /** topic 名的正则，与 [topics] 二选一。 */
    val topicPattern: String = "",
    /** 消费组，`group-offsets` 模式与 [commitOffsetsOnCheckpoint] 需要。 */
    val groupId: String = "",
    /** 透传给 KafkaConsumer 的属性，对应 Flink 的 `properties.*`，例如 `security.protocol`。 */
    val properties: Map<String, String> = emptyMap(),
    /** `earliest-offset`(默认) / `latest-offset` / `group-offsets` / `specific-offsets` / `timestamp`。 */
    val scanStartupMode: String = "earliest-offset",
    /** `scan_startup_mode=specific-offsets` 用：`"topic:partition" -> offset`。 */
    val scanStartupSpecificOffsets: Map<String, Long> = emptyMap(),
    /** `scan_startup_mode=timestamp` 用：起始毫秒时间戳。 */
    val scanStartupTimestampMillis: Long? = null,
    /** `latest-offset`(默认) / `unbounded` / `group-offsets` / `specific-offsets` / `timestamp`。 */
    val scanBoundedMode: String = "latest-offset",
    /** `scan_bounded_mode=specific-offsets` 用：`"topic:partition" -> offset`（不含该 offset）。 */
    val scanBoundedSpecificOffsets: Map<String, Long> = emptyMap(),
    /** `scan_bounded_mode=timestamp` 用：结束毫秒时间戳。 */
    val scanBoundedTimestampMillis: Long? = null,
    /** `string`(默认，按 UTF-8 解码成 STRING) 或 `raw`(原始 BYTES)。 */
    val keyFormat: String = "string",
    /** 同 [keyFormat]。 */
    val valueFormat: String = "string",
    /** 读完一段后把偏移量提交回消费组，需要 [groupId]。仅用于外部监控消费进度，不影响 HData 自身的容错。 */
    val commitOffsetsOnCheckpoint: Boolean = false,
) : Serializable {

    val bounded: Boolean get() = scanBoundedMode != UNBOUNDED

    fun validate() {
        require(bootstrapServers.isNotBlank()) { "bootstrap_servers 不能为空" }
        require(bootstrapServers.split(',').none { it.isBlank() }) { "bootstrap_servers 不能包含空节点" }
        require(topics.isNotEmpty() || topicPattern.isNotBlank()) { "topics 与 topic_pattern 至少要填一个" }
        require(topics.isEmpty() || topicPattern.isBlank()) { "topics 与 topic_pattern 只能填一个" }
        require(topics.none { it.isBlank() }) { "topics 不能包含空 topic" }
        if (topicPattern.isNotBlank()) {
            runCatching { Regex(topicPattern) }
                .onFailure { throw IllegalArgumentException("topic_pattern 不是合法正则: $topicPattern", it) }
        }
        require(scanStartupMode in STARTUP_MODES) {
            "scan_startup_mode 取值非法: $scanStartupMode，可选 ${STARTUP_MODES.joinToString()}"
        }
        require(scanBoundedMode in BOUNDED_MODES) {
            "scan_bounded_mode 取值非法: $scanBoundedMode，可选 ${BOUNDED_MODES.joinToString()}"
        }
        KafkaFormats.of(keyFormat, "key_format")
        KafkaFormats.of(valueFormat, "value_format")

        if (scanStartupMode == TIMESTAMP) {
            requireNotNull(scanStartupTimestampMillis) { "scan_startup_mode=timestamp 需要 scan_startup_timestamp_millis" }
            require(scanStartupTimestampMillis >= 0) { "scan_startup_timestamp_millis 不能为负" }
        }
        if (scanBoundedMode == TIMESTAMP) {
            requireNotNull(scanBoundedTimestampMillis) { "scan_bounded_mode=timestamp 需要 scan_bounded_timestamp_millis" }
            require(scanBoundedTimestampMillis >= 0) { "scan_bounded_timestamp_millis 不能为负" }
        }
        if (scanStartupMode == SPECIFIC_OFFSETS) {
            require(scanStartupSpecificOffsets.isNotEmpty()) {
                "scan_startup_mode=specific-offsets 需要 scan_startup_specific_offsets"
            }
            validateOffsets(scanStartupSpecificOffsets, "scan_startup_specific_offsets")
        } else {
            require(scanStartupSpecificOffsets.isEmpty()) {
                "scan_startup_specific_offsets 只在 scan_startup_mode=specific-offsets 时生效，请从配置中移除"
            }
        }
        if (scanBoundedMode == SPECIFIC_OFFSETS) {
            require(scanBoundedSpecificOffsets.isNotEmpty()) {
                "scan_bounded_mode=specific-offsets 需要 scan_bounded_specific_offsets"
            }
            validateOffsets(scanBoundedSpecificOffsets, "scan_bounded_specific_offsets")
        } else {
            require(scanBoundedSpecificOffsets.isEmpty()) {
                "scan_bounded_specific_offsets 只在 scan_bounded_mode=specific-offsets 时生效，请从配置中移除"
            }
        }
        if (scanStartupMode != TIMESTAMP) {
            require(scanStartupTimestampMillis == null) {
                "scan_startup_timestamp_millis 只在 scan_startup_mode=timestamp 时生效，请从配置中移除"
            }
        }
        if (scanBoundedMode != TIMESTAMP) {
            require(scanBoundedTimestampMillis == null) {
                "scan_bounded_timestamp_millis 只在 scan_bounded_mode=timestamp 时生效，请从配置中移除"
            }
        }
        if (scanStartupMode == GROUP_OFFSETS || scanBoundedMode == GROUP_OFFSETS || commitOffsetsOnCheckpoint) {
            require(groupId.isNotBlank()) { "group-offsets 模式与 commit_offsets_on_checkpoint 都需要 group_id" }
        }
    }

    private fun validateOffsets(offsets: Map<String, Long>, key: String) {
        offsets.forEach { (partition, offset) ->
            val topic = partition.substringBeforeLast(':', "")
            val number = partition.substringAfterLast(':', "").toIntOrNull()
            require(topic.isNotBlank() && number != null && number >= 0) {
                "$key 的键必须是 topic:非负分区号，收到: $partition"
            }
            require(offset >= 0) { "$key 的偏移量不能为负: $partition=$offset" }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1

        const val EARLIEST_OFFSET = "earliest-offset"
        const val LATEST_OFFSET = "latest-offset"
        const val GROUP_OFFSETS = "group-offsets"
        const val SPECIFIC_OFFSETS = "specific-offsets"
        const val TIMESTAMP = "timestamp"
        const val UNBOUNDED = "unbounded"

        val STARTUP_MODES = listOf(EARLIEST_OFFSET, LATEST_OFFSET, GROUP_OFFSETS, SPECIFIC_OFFSETS, TIMESTAMP)
        val BOUNDED_MODES = listOf(UNBOUNDED, LATEST_OFFSET, GROUP_OFFSETS, SPECIFIC_OFFSETS, TIMESTAMP)
    }
}
