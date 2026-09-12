package me.jayer.hdata.sqs

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.sqs.transform.SQSReadFn
import me.jayer.hdata.sqs.transform.SQS_READ_SCHEMA
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `ReadFromSQS`: reads messages from an Amazon SQS queue as a batch or stream via long-polling.
 *
 * @author wuya
 */
class SQSReadProvider : TypedTransformProvider<SQSReadConfig>(SQSReadConfig::class.java) {

    override fun identifier(): String = "ReadFromSQS"

    override fun description(): String = "Read messages from Amazon SQS as a batch or stream"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(
        config: SQSReadConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return SQSSource(config)
    }
}

private class SQSSource(private val config: SQSReadConfig) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        return begin
            .apply("Trigger", Create.of(listOf(1)))
            .apply("ReadFromSQS", ParDo.of(SQSReadFn(config)))
            .setRowSchema(SQS_READ_SCHEMA)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
