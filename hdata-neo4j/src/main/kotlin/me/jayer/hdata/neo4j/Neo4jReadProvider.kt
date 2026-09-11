package me.jayer.hdata.neo4j

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.neo4j.internal.parseSchemaFields
import me.jayer.hdata.neo4j.transform.Neo4jReadFn
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `ReadFromNeo4j` executes a Cypher query and maps results to rows.
 *
 * `Create.of(listOf(""))` triggers one bounded-snapshot read. The DoFn creates its own connection, keeping it serializable.
 *
 * @author wuya
 */
class Neo4jReadProvider : TypedTransformProvider<Neo4jReadConfig>(Neo4jReadConfig::class.java) {

    override fun identifier(): String = "ReadFromNeo4j"

    override fun description(): String = "Read Neo4j with a Cypher query"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(config: Neo4jReadConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return Neo4jSource(config)
    }
}

private class Neo4jSource(private val config: Neo4jReadConfig) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        val schemaFields = parseSchemaFields(config.schemaFields)
        val schema = config.outputSchema()
        val trigger = begin.apply("Trigger", Create.of(listOf("")))
        return trigger.apply("Read", ParDo.of(Neo4jReadFn(config, schema, schemaFields)))
            .setRowSchema(schema)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
