package me.jayer.hdata.kafka

import java.io.Serializable

/**
 * `WriteToKafka` 的配置，键名对齐 Flink Kafka connector 的 sink 侧。
 *
 * ```yaml
 * - type: WriteToKafka
 *   config:
 *     bootstrap_servers: "localhost:9092"
 *     topic: orders
 * ```
 *
 * 输入行需要一个 `value` 字段（类型跟 [valueFormat] 对应：`string` -> STRING，`raw` -> BYTES）；
 * `key` 字段可选，没有就发 null key（由 broker 轮询分区）。[topic] 留空时按行里的 `topic`
 * 字段路由，这样 `ReadFromKafka` 的输出可以直接接过来做跨集群搬运。
 *
 * @author wuya
 */
data class KafkaWriteConfig(
    val bootstrapServers: String = "",
    /** 目标 topic；留空表示按输入行的 `topic` 字段路由。 */
    val topic: String = "",
    /** 透传给 KafkaProducer 的属性，对应 Flink 的 `properties.*`，例如 `compression.type`。 */
    val properties: Map<String, String> = emptyMap(),
    /** 攒够这么多条就 flush 一次并检查发送结果，同时也是最大在途条数。 */
    val batchSize: Int = 1000,
    /** `string`(默认) 或 `raw`，需与输入行 `key` 字段的类型一致。 */
    val keyFormat: String = "string",
    /** `string`(默认) 或 `raw`，需与输入行 `value` 字段的类型一致。 */
    val valueFormat: String = "string",
    /** `at-least-once`(默认，acks=all) 或 `none`(acks=0，发出去就算数)。 */
    val sinkDeliveryGuarantee: String = AT_LEAST_ONCE,
) : Serializable {

    fun validate() {
        require(bootstrapServers.isNotBlank()) { "bootstrap_servers 不能为空" }
        require(batchSize > 0) { "batch_size 必须 > 0" }
        KafkaFormats.of(keyFormat, "key_format")
        KafkaFormats.of(valueFormat, "value_format")
        require(sinkDeliveryGuarantee in DELIVERY_GUARANTEES) {
            if (sinkDeliveryGuarantee == EXACTLY_ONCE) {
                "sink_delivery_guarantee 暂不支持 exactly-once：它需要 Kafka 事务与 Beam 的两阶段提交配合，" +
                    "而 HData 的死信流要求逐条拿到发送结果，两者还没打通。请用 at-least-once 并在下游去重"
            } else {
                "sink_delivery_guarantee 取值非法: $sinkDeliveryGuarantee，可选 ${DELIVERY_GUARANTEES.joinToString()}"
            }
        }
    }

    /** 发送用的 producer 属性，用户在 [properties] 里的设置优先级最高。 */
    fun producerProperties(): Map<String, String> = buildMap {
        put("bootstrap.servers", bootstrapServers)
        put("acks", if (sinkDeliveryGuarantee == NONE) "0" else "all")
        putAll(properties)
    }

    companion object {
        private const val serialVersionUID: Long = 1

        const val NONE = "none"
        const val AT_LEAST_ONCE = "at-least-once"
        const val EXACTLY_ONCE = "exactly-once"

        val DELIVERY_GUARANTEES = listOf(NONE, AT_LEAST_ONCE)
    }
}
