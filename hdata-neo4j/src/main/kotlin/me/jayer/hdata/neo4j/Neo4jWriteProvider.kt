package me.jayer.hdata.neo4j

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.neo4j.transform.Neo4jWriteFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `WriteToNeo4j` executes Cypher writes with dead-letter support.
 *
 * @author wuya
 */
class Neo4jWriteProvider : TypedTransformProvider<Neo4jWriteConfig>(Neo4jWriteConfig::class.java) {

    override fun identifier(): String = "WriteToNeo4j"

    override fun description(): String = "Write Neo4j with Cypher statements and bound parameters"

    override fun supportTier(): ConnectorSupportTier = ConnectorSupportTier.EXPERIMENTAL

    override fun outputCollectionNames(): List<String> = listOf(Tags.ERROR_OUTPUT)

    override fun create(config: Neo4jWriteConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return Neo4jSink(config, context.errorHandling != null, context.transformName)
    }
}

private class Neo4jSink(
    private val config: Neo4jWriteConfig,
    private val deadLetter: Boolean,
    private val transformName: String,
) : RowSink() {

    override fun write(input: PCollection<Row>): PCollection<Row>? {
        val errorSchema = ErrorSchemas.of(input.schema)
        val errors = input
            .apply("Write", ParDo.of(Neo4jWriteFn(config, errorSchema, deadLetter, transformName)))
            .setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
