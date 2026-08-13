package me.jayer.hdata.kafka.transform

import me.jayer.hdata.kafka.KafkaTopicPartition
import me.jayer.hdata.kafka.kafkaRecordToRow
import me.jayer.hdata.kafka.toTp
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.values.Row
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties

/**
 * 按分区切偏移量范围并行读 Kafka，是标准的 Splittable DoFn：
 * 元素是一个 `KafkaTopicPartition`，限制用 `OffsetRange` 表示 `[start, end)`，
 * 交给 Beam 在运行时按 [offsetSplitSize] 切成多段分别读。
 *
 * 读取本身不可中断续跑，所以 `@ProcessElement` 用 `tryClaim(range.to - 1)` 一次性认领整段，
 * 中途放弃会重读该段（at-least-once）。
 *
 * @author wuya
 */
@DoFn.BoundedPerElement
class KafkaReadFn(
    private val bootstrapServers: String,
    private val groupId: String,
    private val consumerConfig: Map<String, String>,
    private val startupMode: String,
    private val specificOffsets: Map<String, Long>,
    private val startupTimestampMillis: Long?,
    private val offsetSplitSize: Long,
) : DoFn<KafkaTopicPartition, Row>() {

    @Transient
    private var consumer: KafkaConsumer<String, String>? = null

    @Setup
    fun setup() {
        consumer = newConsumer()
    }

    @Teardown
    fun tearDown() {
        runCatching { consumer?.close() }
        consumer = null
    }

    @GetInitialRestriction
    fun getInitialRestriction(@Element tp: KafkaTopicPartition): OffsetRange =
        withConsumer { c ->
            val start = resolveStartOffset(c, tp)
            val end = c.endOffsets(listOf(tp.toTp()))[tp.toTp()] ?: 0L
            LOGGER.info("topic[{}] partition[{}] 可读范围 start={}, end={}", tp.topic, tp.partition, start, end)
            if (end <= start) OffsetRange(0, 0) else OffsetRange(start, end)
        }

    @SplitRestriction
    fun splitRestriction(
        @Element tp: KafkaTopicPartition,
        @Restriction restriction: OffsetRange,
        receiver: OutputReceiver<OffsetRange>,
    ) {
        val span = restriction.to - restriction.from
        if (span <= 0) {
            return
        }
        val perSplit = offsetSplitSize.coerceAtLeast(1)
        restriction.split(perSplit, 1).forEach { receiver.output(it) }
        LOGGER.info("topic[{}] partition[{}] 切分为 {} 段", tp.topic, tp.partition, restriction.split(perSplit, 1).size)
    }

    @ProcessElement
    fun processElement(
        @Element tp: KafkaTopicPartition,
        tracker: RestrictionTracker<OffsetRange, Long>,
        receiver: OutputReceiver<Row>,
    ) {
        val range = tracker.currentRestriction()
        if (range.to <= range.from) {
            return
        }
        if (!tracker.tryClaim(range.to - 1)) {
            return
        }
        val c = checkNotNull(consumer) { "消费者未初始化" }
        c.assign(listOf(tp.toTp()))
        c.seek(tp.toTp(), range.from)
        var offset = range.from
        var count = 0L
        while (offset < range.to) {
            val records = c.poll(Duration.ofSeconds(5)) ?: break
            if (records.isEmpty) break
            for (rec: ConsumerRecord<String, String> in records) {
                if (rec.offset() >= range.to) {
                    offset = range.to
                    break
                }
                receiver.output(toRow(rec))
                offset = rec.offset() + 1
                count++
            }
        }
        RECORDS_READ.inc(count)
        LOGGER.info("topic[{}] partition[{}] 区间 [{}, {}) 读完 {} 条", tp.topic, tp.partition, range.from, range.to, count)
    }

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = restriction.newTracker()

    @GetRestrictionCoder
    fun restrictionCoder(): Coder<OffsetRange> = OffsetRange.Coder()

    private fun resolveStartOffset(consumer: KafkaConsumer<String, String>, tp: KafkaTopicPartition): Long =
        when (startupMode) {
            "latest-offset" -> consumer.endOffsets(listOf(tp.toTp()))[tp.toTp()] ?: 0L
            "group-offsets" -> consumer.committed(setOf(tp.toTp()))[tp.toTp()]?.offset() ?: consumer.beginningOffsets(listOf(tp.toTp()))[tp.toTp()] ?: 0L
            "specific-offsets" -> specificOffsets["${tp.topic}:${tp.partition}"] ?: consumer.beginningOffsets(listOf(tp.toTp()))[tp.toTp()] ?: 0L
            "timestamp" -> {
                val ts = checkNotNull(startupTimestampMillis) { "scan_startup_mode=timestamp 需要 scan_startup_timestamp_millis" }
                consumer.offsetsForTimes(mapOf(tp.toTp() to ts))[tp.toTp()]?.offset() ?: consumer.beginningOffsets(listOf(tp.toTp()))[tp.toTp()] ?: 0L
            }
            else -> consumer.beginningOffsets(listOf(tp.toTp()))[tp.toTp()] ?: 0L
        }

    private fun toRow(rec: ConsumerRecord<String, String>): Row =
        kafkaRecordToRow(rec)

    private fun newConsumer(): KafkaConsumer<String, String> {
        val props = Properties().apply {
            this["bootstrap.servers"] = bootstrapServers
            this["key.deserializer"] = StringDeserializer::class.java.name
            this["value.deserializer"] = StringDeserializer::class.java.name
            this["group.id"] = if (groupId.isNotBlank()) groupId else "hdata-kafka-read"
            this["enable.auto.commit"] = "false"
            this["auto.offset.reset"] = OffsetResetStrategy.EARLIEST.name.lowercase()
            consumerConfig.forEach { (k, v) -> this[k] = v }
        }
        return KafkaConsumer(props)
    }

    private fun <T> withConsumer(block: (KafkaConsumer<String, String>) -> T): T {
        val c = newConsumer()
        try {
            return block(c)
        } finally {
            runCatching { c.close() }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(KafkaReadFn::class.java)
        private val RECORDS_READ = Metrics.counter(KafkaReadFn::class.java, "records_read")
    }
}
