package me.jayer.hdata.pulsar

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Schema as PulsarSchema

class PulsarReadProvider : TypedTransformProvider<PulsarReadConfig>(PulsarReadConfig::class.java) {
    override fun identifier() = "ReadFromPulsar"
    override fun description() = "Read a bounded snapshot from Pulsar topics"
    override fun inputCollectionNames() = emptyList<String>()
    override fun create(config: PulsarReadConfig, context: TransformConfig) = PulsarSource(config.also { it.validate() })
}

class PulsarSource(private val config: PulsarReadConfig) : RowSource() {
    override fun read(begin: PBegin): PCollection<Row> = begin
        .apply("Topic", Create.of(config.topic))
        .apply("Read", ParDo.of(PulsarReadFn(config)))
        .setRowSchema(SCHEMA)
}

/**
 * Reads one bounded topic snapshot as an SDF. Pulsar's reader cursor is local to this stateless
 * snapshot connector, so the restriction deliberately records emitted-message count rather than
 * pretending it is a durable broker cursor. A persistent streaming source needs a subscription
 * and acknowledgement policy and is intentionally a separate feature.
 */
@DoFn.BoundedPerElement
internal class PulsarReadFn(private val config: PulsarReadConfig) : DoFn<String, Row>() {
    @GetInitialRestriction
    fun getInitialRestriction(): OffsetRange = OffsetRange(0, config.maxMessages.takeIf { it > 0 } ?: Long.MAX_VALUE)

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = OffsetRangeTracker(restriction)

    @ProcessElement
    fun processElement(
        @Element topic: String,
        tracker: RestrictionTracker<OffsetRange, Long>,
        output: OutputReceiver<Row>,
    ) {
        PulsarClient.builder().serviceUrl(config.serviceUrl).build().use { client ->
            val start = if (config.startPosition == PulsarReadConfig.EARLIEST) MessageId.earliest else MessageId.latest
            client.newReader(PulsarSchema.BYTES).topic(topic).startMessageId(start).create().use { reader ->
                var position = tracker.currentRestriction().from
                while (true) {
                    val message = reader.readNext(
                        config.receiveTimeoutMillis.coerceAtMost(Int.MAX_VALUE.toLong()).toInt(),
                        java.util.concurrent.TimeUnit.MILLISECONDS,
                    ) ?: run {
                        // Record the terminal attempt for an early empty topic; this is required
                        // by OffsetRangeTracker.checkDone().
                        tracker.tryClaim(tracker.currentRestriction().to)
                        return
                    }
                    if (!tracker.tryClaim(position)) return
                    output.outputWithTimestamp(
                        Row.withSchema(SCHEMA).addValue(topic).addValue(message.data).build(),
                        org.joda.time.Instant(message.publishTime),
                    )
                    position++
                }
            }
        }
    }
}

private val SCHEMA: Schema = Schema.builder().addStringField("topic").addByteArrayField("value").build()
