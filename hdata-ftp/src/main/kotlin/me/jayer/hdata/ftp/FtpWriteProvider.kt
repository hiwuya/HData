package me.jayer.hdata.ftp

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.DeliveryCapabilities
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.OrderingScope
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.ftp.transform.FtpWriteFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `WriteToFtp`: writes input rows to sharded files on FTP, with dead-letter output support.
 */
class FtpWriteProvider : TypedTransformProvider<FtpWriteConfig>(FtpWriteConfig::class.java) {

    override fun identifier(): String = "WriteToFtp"

    override fun description(): String = "Write input rows to sharded files on FTP, with dead-letter output support"

    override fun supportTier(): ConnectorSupportTier = ConnectorSupportTier.LOGIC_TESTED_ONLY

    override fun deliveryCapabilities(config: TransformConfig) = DeliveryCapabilities(
        DeliveryMode.AT_LEAST_ONCE, ReplayBehavior.NOT_APPLICABLE, OrderingScope.NONE,
        requiresIdempotencyKey = true,
        notes = "Each bundle uses a new UUID shard name. A bundle retry can leave an additional final file, so downstream consumers must deduplicate or replace the output directory.",
    )

    override fun outputCollectionNames(): List<String> = listOf(Tags.ERROR_OUTPUT)

    override fun create(
        config: FtpWriteConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return FtpSink(config, context.errorHandling != null, context.transformName)
    }
}

private class FtpSink(
    private val config: FtpWriteConfig,
    private val deadLetter: Boolean,
    private val transformName: String,
) : RowSink() {

    override fun write(input: PCollection<Row>): PCollection<Row>? {
        val errorSchema = ErrorSchemas.of(input.schema)
        val errors = input
            .apply("Write", ParDo.of(FtpWriteFn(config, errorSchema, deadLetter, transformName)))
            .setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
