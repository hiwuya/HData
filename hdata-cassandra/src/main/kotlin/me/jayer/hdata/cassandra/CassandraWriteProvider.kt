package me.jayer.hdata.cassandra

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.cassandra.transform.CassandraWriteFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `WriteToCassandra`: writes rows to a Cassandra table in batches, with retries and
 * dead-letter support.
 *
 * @author wuya
 */
class CassandraWriteProvider : TypedTransformProvider<CassandraWriteConfig>(CassandraWriteConfig::class.java) {

    override fun identifier(): String = "WriteToCassandra"

    override fun description(): String = "Write to Cassandra in batches with dead-letter output"

    override fun supportTier(): ConnectorSupportTier = ConnectorSupportTier.EXPERIMENTAL

    override fun outputCollectionNames(): List<String> = listOf(Tags.ERROR_OUTPUT)

    override fun create(
        config: CassandraWriteConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return CassandraSink(config, context.errorHandling != null, context.transformName)
    }
}

private class CassandraSink(
    private val config: CassandraWriteConfig,
    private val deadLetter: Boolean,
    private val transformName: String,
) : RowSink() {

    override fun write(input: PCollection<Row>): PCollection<Row>? {
        val errorSchema = ErrorSchemas.of(input.schema)
        val errors = input
            .apply("Write", ParDo.of(CassandraWriteFn(config, errorSchema, deadLetter, transformName)))
            .setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
