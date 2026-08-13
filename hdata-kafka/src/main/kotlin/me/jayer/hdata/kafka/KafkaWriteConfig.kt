package me.jayer.hdata.kafka

import java.io.Serializable

/**
 * `WriteToKafka` 的配置。配置键对齐 Flink Kafka connector 的 sink 侧。
 *
 * ```yaml
 * - type: WriteToKafka
 *   config:
 *     bootstrap_servers: "localhost:9092"
 *     topic: orders
 *     batch_size: 1000
 * ```
 *
 * 输入行必须包含 `value`(STRING) 字段，可选 `key`(STRING) 字段；其余字段忽略。
 */
data class KafkaWriteConfig(
    val bootstrapServers: String = "",
    val topic: String = "",
    /** 透传给 KafkaProducer 的额外属性，例如 `acks` / `compression.type`。 */
    val producerConfig: Map<String, String> = emptyMap(),
    val batchSize: Int = 1000,
    val keyFormat: String = "string",
    val valueFormat: String = "string",
) : Serializable {

    fun validate() {
        require(bootstrapServers.isNotBlank()) { "bootstrap_servers 不能为空" }
        require(topic.isNotBlank()) { "topic 不能为空" }
        require(batchSize > 0) { "batch_size 必须 > 0" }
    }
}
