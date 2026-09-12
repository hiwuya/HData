package me.jayer.hdata.pulsar

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.core.spi.DeliveryCapabilities
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.OrderingScope
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.TypedTransformProvider
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Schema

class PulsarWriteProvider : TypedTransformProvider<PulsarWriteConfig>(PulsarWriteConfig::class.java) {
    override fun identifier() = "WriteToPulsar"
    override fun description() = "Write rows to a Pulsar topic"
    override fun supportTier(): ConnectorSupportTier = ConnectorSupportTier.EXPERIMENTAL
    override fun deliveryCapabilities(config: TransformConfig) = DeliveryCapabilities(
        DeliveryMode.AT_LEAST_ONCE, ReplayBehavior.NOT_APPLICABLE, OrderingScope.NONE,
        requiresIdempotencyKey = true,
        notes = "A successful broker publish can be retried after an ambiguous acknowledgement; provide a deduplication boundary for replay safety.",
    )
    override fun outputCollectionNames() = listOf(Tags.ERROR_OUTPUT)
    override fun create(config: PulsarWriteConfig, context: TransformConfig) = PulsarSink(config.also { it.validate() }, context.errorHandling != null, context.transformName)
}

class PulsarSink(
    private val config: PulsarWriteConfig,
    private val deadLetter: Boolean,
    private val transformName: String,
) : RowSink() {
    override fun write(input: PCollection<Row>): PCollection<Row>? {
        val errorSchema = ErrorSchemas.of(input.schema)
        val errors = input.apply("Write", ParDo.of(PulsarWriteFn(config, errorSchema, deadLetter, transformName))).setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }
}

internal class PulsarWriteFn(
    private val config: PulsarWriteConfig,
    private val errorSchema: org.apache.beam.sdk.schemas.Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
) : DoFn<Row, Row>() {
    @Transient private var client: PulsarClient? = null
    @Transient private var producer: org.apache.pulsar.client.api.Producer<ByteArray>? = null
    private val failures = mutableListOf<org.apache.beam.sdk.values.ValueInSingleWindow<Row>>()

    @Setup
    fun setup() {
        client = PulsarClient.builder().serviceUrl(config.serviceUrl).build()
        producer = client!!.newProducer(Schema.BYTES).topic(config.topic).create()
    }

    @ProcessElement
    fun processElement(
        @Element row: Row,
        @Timestamp timestamp: org.joda.time.Instant,
        window: org.apache.beam.sdk.transforms.windowing.BoundedWindow,
        pane: org.apache.beam.sdk.transforms.windowing.PaneInfo,
    ) {
        val record = org.apache.beam.sdk.values.ValueInSingleWindow.of(row, timestamp, window, pane)
        try {
            require(row.schema.hasField(config.valueField)) { "input row is missing Pulsar value field ${config.valueField}" }
            val value = row.getValue<Any?>(config.valueField)
            require(value != null) { "Pulsar value field ${config.valueField} must not be null" }
            val bytes = when (value) {
                is ByteArray -> value
                is String -> value.toByteArray(Charsets.UTF_8)
                else -> value.toString().toByteArray(Charsets.UTF_8)
            }
            producer!!.newMessage().value(bytes).send()
        } catch (e: Exception) {
            if (!deadLetter) throw e
            failures += org.apache.beam.sdk.values.ValueInSingleWindow.of(
                ErrorSchemas.failure(errorSchema, row, e, transformName), timestamp, window, pane,
            )
        }
    }

    @FinishBundle
    fun finishBundle(context: FinishBundleContext) {
        failures.forEach { context.output(it.value, it.timestamp, it.window) }
        failures.clear()
    }

    @Teardown
    fun teardown() { runCatching { producer?.close() }; runCatching { client?.close() }; producer = null; client = null }
}
