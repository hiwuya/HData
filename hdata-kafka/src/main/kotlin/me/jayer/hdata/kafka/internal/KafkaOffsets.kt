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
 * Translate the startup/bounded modes from the config into per-partition start/stop
 * offsets at **graph-construction time**, producing the [KafkaSourceDescriptor] list
 * handed to Beam's `ReadFromKafkaDoFn`.
 *
 * The reason to decide this at graph-construction time rather than discover it at
 * runtime like `KafkaIO.read().withTopics(...)`: among Flink's 5 startup modes,
 * `specific-offsets` and `group-offsets` both need a per-partition start point, which
 * `KafkaIO.Read`'s public API cannot express — it only understands `auto.offset.reset`
 * and a single global `startReadTime`.
 *
 * The cost is that the machine submitting the job must be able to reach the broker —
 * same as before this refactor.
 *
 * @author wuya
 */
internal object KafkaOffsets {

    private val LOGGER = LoggerFactory.getLogger(KafkaOffsets::class.java)

    /** The offset range to read for a single partition; [stop] == null means read to the end of time (streaming). */
    data class PartitionRange(val partition: TopicPartition, val start: Long, val stop: Long?)

    fun resolve(config: KafkaReadConfig): List<KafkaSourceDescriptor> {
        val servers = config.bootstrapServers.split(",").map { it.trim() }.filter { it.isNotEmpty() }
        return newConsumer(config).use { consumer -> ranges(config, consumer) }
            .map { KafkaSourceDescriptor.of(it.partition, it.start, null, it.stop, null, servers) }
    }

    /**
     * Only depends on the [Consumer] interface, so tests can inject a `MockConsumer`.
     *
     * The returned ranges already filter out partitions where `start >= stop` — those
     * partitions have no data to read, and including them would only spin up a pointless
     * read unit.
     */
    fun ranges(config: KafkaReadConfig, consumer: Consumer<ByteArray, ByteArray>): List<PartitionRange> {
        val partitions = listPartitions(config, consumer)
        require(partitions.isNotEmpty()) {
            "No partitions found; check whether topics=${config.topics} / topic_pattern=${config.topicPattern} exist"
        }
        val starts = startOffsets(config, consumer, partitions)
        val stops = stopOffsets(config, consumer, partitions)

        return partitions.mapNotNull { tp ->
            val start = starts.getValue(tp)
            val stop = stops?.getValue(tp)
            if (stop != null && stop <= start) {
                LOGGER.info("{}-{} start {} >= stop {}; no data to read, skipping", tp.topic(), tp.partition(), start, stop)
                null
            } else {
                LOGGER.info("{}-{} read range [{}, {})", tp.topic(), tp.partition(), start, stop ?: "∞")
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
            require(!infos.isNullOrEmpty()) { "topic[$topic] does not exist or has no partitions" }
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
        else -> throw IllegalArgumentException("scan_startup_mode is invalid: ${config.scanStartupMode}")
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
        else -> throw IllegalArgumentException("scan_bounded_mode is invalid: ${config.scanBoundedMode}")
    }

    /**
     * Partitions whose consumer group has never committed an offset fall back to [fallback].
     *
     * Before this refactor this used `consumer.committed(...)`, but the read side never
     * commits offsets, so `group-offsets` could never get a value and always read from the
     * start — the parameter was effectively a no-op. Committing is now handled by
     * `commit_offsets_on_checkpoint`.
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
        LOGGER.warn("Consumer group has no committed offset for {}; these partitions fall back to the default start", missing)
        val defaults = fallback()
        return partitions.associateWith { committed[it]?.offset() ?: defaults.getValue(it) }
    }

    private fun specific(
        offsets: Map<String, Long>,
        partitions: List<TopicPartition>,
        configKey: String,
    ): Map<TopicPartition, Long> {
        val expected = partitions.mapTo(linkedSetOf()) { "${it.topic()}:${it.partition()}" }
        val extra = offsets.keys - expected
        require(extra.isEmpty()) {
            "$configKey contains partitions not present in the current subscription: ${extra.sorted()}"
        }
        return partitions.associateWith { tp ->
            val key = "${tp.topic()}:${tp.partition()}"
            requireNotNull(offsets[key]) {
                "$configKey is missing the offset for partition $key; all ${partitions.size} partitions must be specified — " +
                    "leaving one out means that partition is read incorrectly or not at all"
            }
        }
    }

    /**
     * Locate by timestamp. When a partition has no messages after this point in time,
     * [fallback] provides the way out (the start uses endOffsets to mean "wait for new
     * messages from now on", the stop uses endOffsets to mean "read up to the current end").
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
        LOGGER.warn("Partition {} has no messages after {}; falling back to the current end offset", missing, timestampMillis)
        val defaults = fallback()
        return partitions.associateWith { found[it]?.offset() ?: defaults.getValue(it) }
    }

    fun consumerProperties(config: KafkaReadConfig): Map<String, Any> = buildMap {
        putAll(config.properties)
        put("bootstrap.servers", config.bootstrapServers)
        // Beam's SDF manages offsets itself; auto-commit would only scramble the progress
        put("enable.auto.commit", "false")
        putIfAbsent("auto.offset.reset", "none")
        if (config.groupId.isNotBlank()) {
            put("group.id", config.groupId)
        }
    }

    private fun newConsumer(config: KafkaReadConfig): KafkaConsumer<ByteArray, ByteArray> {
        val props = Properties().apply {
            putAll(consumerProperties(config))
            this["key.deserializer"] = ByteArrayDeserializer::class.java.name
            this["value.deserializer"] = ByteArrayDeserializer::class.java.name
            // Use a dedicated client.id for metadata probing so it doesn't mix with the read-side consumer in monitoring
            this["client.id"] = "hdata-kafka-metadata"
        }
        return KafkaConsumer(props)
    }
}
