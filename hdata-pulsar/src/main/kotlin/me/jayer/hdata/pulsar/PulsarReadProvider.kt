package me.jayer.hdata.pulsar

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
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

private class PulsarReadFn(private val config: PulsarReadConfig) : org.apache.beam.sdk.transforms.DoFn<String, Row>() {
    @ProcessElement
    fun processElement(@Element topic: String, output: OutputReceiver<Row>) {
        PulsarClient.builder().serviceUrl(config.serviceUrl).build().use { client ->
            val start = if (config.startPosition == PulsarReadConfig.EARLIEST) MessageId.earliest else MessageId.latest
            client.newReader(PulsarSchema.BYTES).topic(topic).startMessageId(start).create().use { reader ->
                var count = 0L
                while (config.maxMessages < 0 || count < config.maxMessages) {
                    val message = reader.readNext(
                        config.receiveTimeoutMillis.coerceAtMost(Int.MAX_VALUE.toLong()).toInt(),
                        java.util.concurrent.TimeUnit.MILLISECONDS,
                    ) ?: break
                    output.output(Row.withSchema(SCHEMA).addValue(topic).addValue(message.data).build())
                    count++
                }
            }
        }
    }
}

private val SCHEMA: Schema = Schema.builder().addStringField("topic").addByteArrayField("value").build()
