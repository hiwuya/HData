package me.jayer.hdata.kafka.internal

import me.jayer.hdata.kafka.KafkaReadConfig
import me.jayer.hdata.kafka.KafkaReadConfig.Companion.EARLIEST_OFFSET
import me.jayer.hdata.kafka.KafkaReadConfig.Companion.GROUP_OFFSETS
import me.jayer.hdata.kafka.KafkaReadConfig.Companion.LATEST_OFFSET
import me.jayer.hdata.kafka.KafkaReadConfig.Companion.SPECIFIC_OFFSETS
import me.jayer.hdata.kafka.KafkaReadConfig.Companion.TIMESTAMP
import me.jayer.hdata.kafka.KafkaReadConfig.Companion.UNBOUNDED
import org.apache.beam.sdk.io.kafka.KafkaSourceDescriptor
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.slf4j.LoggerFactory
import java.util.Properties

/**
 * 在**构图阶段**把配置里的 startup/bounded 模式翻译成每个分区的起止偏移量，
 * 产出交给 Beam `ReadFromKafkaDoFn` 的 [KafkaSourceDescriptor] 列表。
 *
 * 之所以在构图阶段就定下来，而不是像 `KafkaIO.read().withTopics(...)` 那样运行期发现：
 * Flink 的 5 种 startup 模式里 `specific-offsets` 与 `group-offsets` 都需要按分区给定起点，
 * `KafkaIO.Read` 的公开 API 只认 `auto.offset.reset` 和一个全局的 `startReadTime`，表达不了。
 * 代价是提交作业的机器必须能连上 broker——这一点与重构前一致。
 *
 * @author wuya
 */
internal object KafkaOffsets {

    private val LOGGER = LoggerFactory.getLogger(KafkaOffsets::class.java)

    /** 一个分区要读的偏移量区间，[stop] 为 null 表示读到天荒地老（流式）。 */
    data class PartitionRange(val partition: TopicPartition, val start: Long, val stop: Long?)

    fun resolve(config: KafkaReadConfig): List<KafkaSourceDescriptor> {
        val servers = config.bootstrapServers.split(",").map { it.trim() }.filter { it.isNotEmpty() }
        return newConsumer(config).use { consumer -> ranges(config, consumer) }
            .map { KafkaSourceDescriptor.of(it.partition, it.start, null, it.stop, null, servers) }
    }

    /**
     * 只吃 [Consumer] 接口，测试可以塞 `MockConsumer` 进来。
     *
     * 返回的区间已经滤掉了 `start >= stop` 的分区——那些分区没有可读数据，
     * 放进去只会白白起一个读取单元。
     */
    fun ranges(config: KafkaReadConfig, consumer: Consumer<ByteArray, ByteArray>): List<PartitionRange> {
        val partitions = listPartitions(config, consumer)
        require(partitions.isNotEmpty()) {
            "没有找到任何分区，检查 topics=${config.topics} / topic_pattern=${config.topicPattern} 是否存在"
        }
        val starts = startOffsets(config, consumer, partitions)
        val stops = stopOffsets(config, consumer, partitions)

        return partitions.mapNotNull { tp ->
            val start = starts.getValue(tp)
            val stop = stops?.getValue(tp)
            if (stop != null && stop <= start) {
                LOGGER.info("{}-{} 起点 {} >= 终点 {}，没有要读的数据，跳过", tp.topic(), tp.partition(), start, stop)
                null
            } else {
                LOGGER.info("{}-{} 读取区间 [{}, {})", tp.topic(), tp.partition(), start, stop ?: "∞")
                PartitionRange(tp, start, stop)
            }
        }
    }

    private fun listPartitions(config: KafkaReadConfig, consumer: Consumer<ByteArray, ByteArray>): List<TopicPartition> {
        val topics = if (config.topicPattern.isNotBlank()) {
            val regex = Regex(config.topicPattern)
            consumer.listTopics().keys.filter { regex.matches(it) }.sorted()
        } else {
            config.topics
        }
        return topics.flatMap { topic ->
            val infos = consumer.partitionsFor(topic)
            require(!infos.isNullOrEmpty()) { "topic[$topic] 不存在或没有分区" }
            infos.map { TopicPartition(topic, it.partition()) }
        }.sortedWith(compareBy({ it.topic() }, { it.partition() }))
    }

    private fun startOffsets(
        config: KafkaReadConfig,
        consumer: Consumer<ByteArray, ByteArray>,
        partitions: List<TopicPartition>,
    ): Map<TopicPartition, Long> = when (config.scanStartupMode) {
        EARLIEST_OFFSET -> consumer.beginningOffsets(partitions).mapValues { it.value }
        LATEST_OFFSET -> consumer.endOffsets(partitions).mapValues { it.value }
        GROUP_OFFSETS -> committedOrElse(consumer, partitions) { consumer.beginningOffsets(partitions) }
        SPECIFIC_OFFSETS -> specific(config.scanStartupSpecificOffsets, partitions, "scan_startup_specific_offsets")
        TIMESTAMP -> forTimes(consumer, partitions, config.scanStartupTimestampMillis!!) {
            consumer.endOffsets(partitions)
        }
        else -> throw IllegalArgumentException("scan_startup_mode 取值非法: ${config.scanStartupMode}")
    }

    private fun stopOffsets(
        config: KafkaReadConfig,
        consumer: Consumer<ByteArray, ByteArray>,
        partitions: List<TopicPartition>,
    ): Map<TopicPartition, Long>? = when (config.scanBoundedMode) {
        UNBOUNDED -> null
        LATEST_OFFSET -> consumer.endOffsets(partitions).mapValues { it.value }
        GROUP_OFFSETS -> committedOrElse(consumer, partitions) { consumer.endOffsets(partitions) }
        SPECIFIC_OFFSETS -> specific(config.scanBoundedSpecificOffsets, partitions, "scan_bounded_specific_offsets")
        TIMESTAMP -> forTimes(consumer, partitions, config.scanBoundedTimestampMillis!!) {
            consumer.endOffsets(partitions)
        }
        else -> throw IllegalArgumentException("scan_bounded_mode 取值非法: ${config.scanBoundedMode}")
    }

    /**
     * 消费组没提交过偏移量的分区回退到 [fallback]。
     *
     * 重构前这里用的是 `consumer.committed(...)`，但读取端从不提交偏移量，于是 `group-offsets`
     * 永远拿不到值、每次都从头读——参数形同虚设。现在提交由 `commit_offsets_on_checkpoint` 负责。
     */
    private fun committedOrElse(
        consumer: Consumer<ByteArray, ByteArray>,
        partitions: List<TopicPartition>,
        fallback: () -> Map<TopicPartition, Long>,
    ): Map<TopicPartition, Long> {
        val committed = consumer.committed(partitions.toSet())
        val missing = partitions.filter { committed[it] == null }
        if (missing.isEmpty()) {
            return partitions.associateWith { committed.getValue(it).offset() }
        }
        LOGGER.warn("消费组没有 {} 的已提交偏移量，这些分区回退到默认起点", missing)
        val defaults = fallback()
        return partitions.associateWith { committed[it]?.offset() ?: defaults.getValue(it) }
    }

    private fun specific(
        offsets: Map<String, Long>,
        partitions: List<TopicPartition>,
        configKey: String,
    ): Map<TopicPartition, Long> = partitions.associateWith { tp ->
        val key = "${tp.topic()}:${tp.partition()}"
        requireNotNull(offsets[key]) {
            "$configKey 缺少分区 $key 的偏移量；需要给全部 ${partitions.size} 个分区都指定，" +
                "漏掉一个就意味着这个分区读不到或读错位置"
        }
    }

    /**
     * 按时间戳定位。该分区在这个时间点之后没有消息时，[fallback] 给出退路
     * （起点用 endOffsets 表示"从此刻起等新消息"，终点用 endOffsets 表示"读到当前末尾"）。
     */
    private fun forTimes(
        consumer: Consumer<ByteArray, ByteArray>,
        partitions: List<TopicPartition>,
        timestampMillis: Long,
        fallback: () -> Map<TopicPartition, Long>,
    ): Map<TopicPartition, Long> {
        val found = consumer.offsetsForTimes(partitions.associateWith { timestampMillis })
        val missing = partitions.filter { found[it] == null }
        if (missing.isEmpty()) {
            return partitions.associateWith { found.getValue(it).offset() }
        }
        LOGGER.warn("分区 {} 在 {} 之后没有消息，回退到当前末尾偏移量", missing, timestampMillis)
        val defaults = fallback()
        return partitions.associateWith { found[it]?.offset() ?: defaults.getValue(it) }
    }

    fun consumerProperties(config: KafkaReadConfig): Map<String, Any> = buildMap {
        put("bootstrap.servers", config.bootstrapServers)
        // Beam 的 SDF 自己管偏移量，自动提交只会把进度写乱
        put("enable.auto.commit", "false")
        put("auto.offset.reset", "none")
        if (config.groupId.isNotBlank()) {
            put("group.id", config.groupId)
        }
        putAll(config.properties)
    }

    private fun newConsumer(config: KafkaReadConfig): KafkaConsumer<ByteArray, ByteArray> {
        val props = Properties().apply {
            putAll(consumerProperties(config))
            this["key.deserializer"] = ByteArrayDeserializer::class.java.name
            this["value.deserializer"] = ByteArrayDeserializer::class.java.name
            // 元数据探测用独立的 client.id，避免和读取端的消费者在监控里混在一起
            this["client.id"] = "hdata-kafka-metadata"
        }
        return KafkaConsumer(props)
    }
}
