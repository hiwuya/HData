package me.jayer.hdata.kafka

import java.io.Serializable

/**
 * Config for `WriteToKafka`; the key names align with the Flink Kafka connector's sink side.
 *
 * ```yaml
 * - type: WriteToKafka
 *   config:
 *     bootstrap_servers: "localhost:9092"
 *     topic: orders
 * ```
 *
 * The input row needs a `value` field (its type corresponds to [valueFormat]: `string` -> STRING, `raw` -> BYTES);
 * the `key` field is optional — if absent, a null key is sent (so the broker round-robins partitions). When
 * [topic] is left blank, routing follows the row's `topic` field, so `ReadFromKafka`'s output can be fed
 * straight in for cross-cluster migration.
 *
 * @author wuya
 */
data class KafkaWriteConfig(
    val bootstrapServers: String = "",
    /** The destination topic; when blank, routing follows the input row's `topic` field. */
    val topic: String = "",
    /**
     * Properties passed through to KafkaProducer, corresponding to Flink's `properties.*`, e.g. `compression.type`.
     * The connection address, serializers, and acks are managed by explicit config and must not be set here again.
     */
    val properties: Map<String, String> = emptyMap(),
    /** Flush once this many records accumulate and check the send results; also the max in-flight count. */
    val batchSize: Int = 1000,
    /** `string` (default) or `raw`; must match the type of the input row's `key` field. */
    val keyFormat: String = "string",
    /** `string` (default) or `raw`; must match the type of the input row's `value` field. */
    val valueFormat: String = "string",
    /** `at-least-once` (default, acks=all) or `none` (acks=0, sent == delivered). */
    val sinkDeliveryGuarantee: String = AT_LEAST_ONCE,
) : Serializable {

    fun validate() {
        require(bootstrapServers.isNotBlank()) { "bootstrap_servers must not be empty" }
        require(bootstrapServers.split(',').none { it.isBlank() }) { "bootstrap_servers must not contain an empty node" }
        require(batchSize > 0) { "batch_size must be > 0" }
        KafkaFormats.of(keyFormat, "key_format")
        KafkaFormats.of(valueFormat, "value_format")
        require(sinkDeliveryGuarantee in DELIVERY_GUARANTEES) {
            if (sinkDeliveryGuarantee == EXACTLY_ONCE) {
                "sink_delivery_guarantee does not yet support exactly-once: it needs Kafka transactions coordinated with " +
                    "Beam's two-phase commit, but HData's dead-letter stream requires per-record send results, and the " +
                    "two are not yet connected. Use at-least-once and deduplicate downstream"
            } else {
                "sink_delivery_guarantee is invalid: $sinkDeliveryGuarantee; allowed values: ${DELIVERY_GUARANTEES.joinToString()}"
            }
        }
        require("bootstrap.servers" !in properties) {
            "properties.bootstrap.servers duplicates bootstrap_servers; please use only bootstrap_servers"
        }
        require("key.serializer" !in properties && "value.serializer" !in properties) {
            "key.serializer/value.serializer are determined by key_format/value_format and cannot be overridden via properties"
        }
        properties["acks"]?.let { configured ->
            val expected = expectedAcks()
            val equivalent = configured == expected || expected == "all" && configured == "-1"
            require(equivalent) {
                "properties.acks=$configured conflicts with sink_delivery_guarantee=$sinkDeliveryGuarantee; " +
                    "expected acks=$expected"
            }
        }
    }

    /** Producer properties for sending; delivery-guarantee-related fields are written last so they cannot be silently overridden by passthrough properties. */
    fun producerProperties(): Map<String, String> = buildMap {
        putAll(properties)
        put("bootstrap.servers", bootstrapServers)
        put("acks", expectedAcks())
    }

    private fun expectedAcks(): String = if (sinkDeliveryGuarantee == NONE) "0" else "all"

    companion object {
        private const val serialVersionUID: Long = 1

        const val NONE = "none"
        const val AT_LEAST_ONCE = "at-least-once"
        const val EXACTLY_ONCE = "exactly-once"

        val DELIVERY_GUARANTEES = listOf(NONE, AT_LEAST_ONCE)
    }
}
