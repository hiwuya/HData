package me.jayer.hdata.jdbc

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.core.spi.DeliveryCapabilities
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.OrderingScope
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.jdbc.transform.JdbcWriteFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `WriteToJdbc`: bulk writes into a relational database.
 *
 * Once `error_handling` is declared, a failed bulk write falls back to per-row writes to locate the bad data, and rows that still
 * cannot be written go to the dead-letter stream (see [ErrorSchemas] for the schema) instead of failing the whole job.
 *
 * @author wuya
 * @date 2022-08-26
 */
class JdbcWriteProvider : TypedTransformProvider<JdbcWriteConfig>(JdbcWriteConfig::class.java) {

    override fun identifier(): String = "WriteToJdbc"

    override fun description(): String = "Bulk writes into a relational database, with dead-letter output"

    override fun outputCollectionNames(): List<String> = listOf(Tags.ERROR_OUTPUT)

    // EXPERIMENTAL: see JdbcReadProvider.supportTier.
    override fun supportTier(): ConnectorSupportTier = ConnectorSupportTier.EXPERIMENTAL

    override fun deliveryCapabilities(config: TransformConfig): DeliveryCapabilities {
        val acceptsDuplicates = config.bind(JdbcWriteConfig::class.java).allowDuplicateReplay
        return DeliveryCapabilities(
            deliveryMode = DeliveryMode.AT_LEAST_ONCE,
            replayBehavior = ReplayBehavior.NOT_APPLICABLE,
            ordering = OrderingScope.NONE,
            requiresIdempotencyKey = !acceptsDuplicates,
            notes = if (acceptsDuplicates) "Plain INSERT duplicate risk explicitly acknowledged by allow_duplicate_replay."
            else "Plain INSERT batches are not an upsert; a bundle retry can re-insert committed rows. Use a unique target key, deduplicate upstream, or explicitly set allow_duplicate_replay when duplicates are acceptable.",
        )
    }

    override fun create(
        config: JdbcWriteConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return JdbcSink(config, context.errorHandling != null, context.transformName)
    }
}

private class JdbcSink(
    private val config: JdbcWriteConfig,
    private val deadLetter: Boolean,
    private val transformName: String,
) : RowSink() {

    override fun write(input: PCollection<Row>): PCollection<Row>? {
        val inputSchema = input.schema
        val errorSchema = ErrorSchemas.of(inputSchema)
        val errors = input
            .apply("Write", ParDo.of(JdbcWriteFn(config, inputSchema, errorSchema, deadLetter, transformName)))
            .setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
