package me.jayer.hdata.kafka

import java.io.Serializable
import org.apache.kafka.common.TopicPartition

/**
 * 一个被切分的读取单元：某个 topic 的某个分区。
 *
 * 真正的偏移量范围由 [me.jayer.hdata.kafka.transform.KafkaReadFn] 在 `@GetInitialRestriction`
 * 阶段通过 consumer 的 `beginningOffsets` / `endOffsets` 确定，所以这里只描述"要读哪个分区"。
 */
data class KafkaTopicPartition(val topic: String, val partition: Int) : Serializable

/** 转成 Kafka 客户端的 [TopicPartition]。 */
fun KafkaTopicPartition.toTp(): TopicPartition = TopicPartition(topic, partition)
