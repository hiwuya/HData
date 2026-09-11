package me.jayer.hdata.kafka

import java.io.Serializable

/**
 * Config for `ReadFromKafka`; the key names align with the Flink Kafka connector's table parameters.
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
 * Two **deliberate differences** from Flink:
 *  1. [scanBoundedMode] defaults to `latest-offset` instead of Flink's `unbounded`. HData is mainly used for
 *     batch sync, so defaulting to a never-ending streaming job would be a nasty surprise; write
 *     `scan_bounded_mode: unbounded` explicitly to consume as a stream.
 *  2. `specific-offsets` offsets are given as `"topic:partition" -> offset` rather than Flink's
 *     `partition:0,offset:42` string, because here a single read can span multiple topics.
 *
 * The emitted row schema is documented in [KafkaFormats.readSchema].
 *
 * @author wuya
 */
data class KafkaReadConfig(
    val bootstrapServers: String = "",
    /** The list of topics to read; choose either this or [topicPattern]. */
    val topics: List<String> = emptyList(),
    /** A regex for topic names; choose either this or [topics]. */
    val topicPattern: String = "",
    /** The consumer group; required by the `group-offsets` mode and [commitOffsetsOnCheckpoint]. */
    val groupId: String = "",
    /** Properties passed through to KafkaConsumer, corresponding to Flink's `properties.*`, e.g. `security.protocol`. */
    val properties: Map<String, String> = emptyMap(),
    /** `earliest-offset` (default) / `latest-offset` / `group-offsets` / `specific-offsets` / `timestamp`. */
    val scanStartupMode: String = "earliest-offset",
    /** Used when `scan_startup_mode=specific-offsets`: `"topic:partition" -> offset`. */
    val scanStartupSpecificOffsets: Map<String, Long> = emptyMap(),
    /** Used when `scan_startup_mode=timestamp`: the starting millisecond timestamp. */
    val scanStartupTimestampMillis: Long? = null,
    /** `latest-offset` (default) / `unbounded` / `group-offsets` / `specific-offsets` / `timestamp`. */
    val scanBoundedMode: String = "latest-offset",
    /** Used when `scan_bounded_mode=specific-offsets`: `"topic:partition" -> offset` (exclusive of that offset). */
    val scanBoundedSpecificOffsets: Map<String, Long> = emptyMap(),
    /** Used when `scan_bounded_mode=timestamp`: the ending millisecond timestamp. */
    val scanBoundedTimestampMillis: Long? = null,
    /** `string` (default, decodes to STRING via UTF-8) or `raw` (raw BYTES). */
    val keyFormat: String = "string",
    /** Same as [keyFormat]. */
    val valueFormat: String = "string",
    /** Commit the offset back to the consumer group after reading a batch; needs [groupId]. Only for external progress monitoring, does not affect HData's own fault tolerance. */
    val commitOffsetsOnCheckpoint: Boolean = false,
) : Serializable {

    val bounded: Boolean get() = scanBoundedMode != UNBOUNDED

    fun validate() {
        require(bootstrapServers.isNotBlank()) { "bootstrap_servers must not be empty" }
        require(bootstrapServers.split(',').none { it.isBlank() }) { "bootstrap_servers must not contain an empty node" }
        require(topics.isNotEmpty() || topicPattern.isNotBlank()) { "at least one of topics and topic_pattern must be set" }
        require(topics.isEmpty() || topicPattern.isBlank()) { "only one of topics and topic_pattern may be set" }
        require(topics.none { it.isBlank() }) { "topics must not contain an empty topic" }
        require(topics.distinct().size == topics.size) { "topics must not repeat, otherwise the same partition would be read multiple times" }
        validateProperties()
        if (topicPattern.isNotBlank()) {
            runCatching { Regex(topicPattern) }
                .onFailure { throw IllegalArgumentException("topic_pattern is not a valid regex: $topicPattern", it) }
        }
        require(scanStartupMode in STARTUP_MODES) {
            "scan_startup_mode is invalid: $scanStartupMode; allowed values: ${STARTUP_MODES.joinToString()}"
        }
        require(scanBoundedMode in BOUNDED_MODES) {
            "scan_bounded_mode is invalid: $scanBoundedMode; allowed values: ${BOUNDED_MODES.joinToString()}"
        }
        KafkaFormats.of(keyFormat, "key_format")
        KafkaFormats.of(valueFormat, "value_format")

        if (scanStartupMode == TIMESTAMP) {
            requireNotNull(scanStartupTimestampMillis) { "scan_startup_mode=timestamp requires scan_startup_timestamp_millis" }
            require(scanStartupTimestampMillis >= 0) { "scan_startup_timestamp_millis must not be negative" }
        }
        if (scanBoundedMode == TIMESTAMP) {
            requireNotNull(scanBoundedTimestampMillis) { "scan_bounded_mode=timestamp requires scan_bounded_timestamp_millis" }
            require(scanBoundedTimestampMillis >= 0) { "scan_bounded_timestamp_millis must not be negative" }
        }
        if (scanStartupMode == TIMESTAMP && scanBoundedMode == TIMESTAMP) {
            require(scanStartupTimestampMillis!! <= scanBoundedTimestampMillis!!) {
                "scan_startup_timestamp_millis must be <= scan_bounded_timestamp_millis"
            }
        }
        if (scanStartupMode == SPECIFIC_OFFSETS) {
            require(scanStartupSpecificOffsets.isNotEmpty()) {
                "scan_startup_mode=specific-offsets requires scan_startup_specific_offsets"
            }
            validateOffsets(scanStartupSpecificOffsets, "scan_startup_specific_offsets")
        } else {
            require(scanStartupSpecificOffsets.isEmpty()) {
                "scan_startup_specific_offsets only takes effect when scan_startup_mode=specific-offsets; please remove it from the config"
            }
        }
        if (scanBoundedMode == SPECIFIC_OFFSETS) {
            require(scanBoundedSpecificOffsets.isNotEmpty()) {
                "scan_bounded_mode=specific-offsets requires scan_bounded_specific_offsets"
            }
            validateOffsets(scanBoundedSpecificOffsets, "scan_bounded_specific_offsets")
        } else {
            require(scanBoundedSpecificOffsets.isEmpty()) {
                "scan_bounded_specific_offsets only takes effect when scan_bounded_mode=specific-offsets; please remove it from the config"
            }
        }
        if (scanStartupMode != TIMESTAMP) {
            require(scanStartupTimestampMillis == null) {
                "scan_startup_timestamp_millis only takes effect when scan_startup_mode=timestamp; please remove it from the config"
            }
        }
        if (scanBoundedMode != TIMESTAMP) {
            require(scanBoundedTimestampMillis == null) {
                "scan_bounded_timestamp_millis only takes effect when scan_bounded_mode=timestamp; please remove it from the config"
            }
        }
        if (scanStartupMode == GROUP_OFFSETS || scanBoundedMode == GROUP_OFFSETS || commitOffsetsOnCheckpoint) {
            require(groupId.isNotBlank()) { "both the group-offsets mode and commit_offsets_on_checkpoint require group_id" }
        }
    }

    private fun validateOffsets(offsets: Map<String, Long>, key: String) {
        offsets.forEach { (partition, offset) ->
            val topic = partition.substringBeforeLast(':', "")
            val number = partition.substringAfterLast(':', "").toIntOrNull()
            require(topic.isNotBlank() && number != null && number >= 0) {
                "$key keys must be topic:non-negative partition number; received: $partition"
            }
            require(offset >= 0) { "$key offset must not be negative: $partition=$offset" }
        }
    }

    private fun validateProperties() {
        require(properties.keys.none { it.isBlank() }) { "properties must not contain an empty key" }
        val reserved = setOf(
            "bootstrap.servers",
            "group.id",
            "key.deserializer",
            "value.deserializer",
            "enable.auto.commit",
        )
        val repeated = properties.keys.intersect(reserved)
        require(repeated.isEmpty()) {
            "${repeated.sorted()} in properties are explicitly managed by HData and must not be set again"
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
