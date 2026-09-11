package me.jayer.hdata.core.transforms

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.core.util.RowConverters
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import tools.jackson.databind.JsonNode

/**
 * Builds data from literals, with the schema inferred from the literals. For testing and integration:
 *
 * ```yaml
 * - type: Create
 *   config:
 *     elements:
 *       - { id: 1, name: "a" }
 *       - { id: 2, name: "b" }
 * ```
 *
 * @author wuya
 * @date 2022-08-30
 */
class CreateProvider : TypedTransformProvider<CreateConfig>(CreateConfig::class.java) {

    override fun identifier(): String = "Create"

    override fun description(): String = "Builds a bounded dataset from literals in the config"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(config: CreateConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        require(config.elements.isNotEmpty()) { "Create requires at least one element in elements" }
        val schema = RowConverters.inferSchema(config.elements, "elements")
        val rows = config.elements.mapIndexed { index, element ->
            RowConverters.toRow(schema, element, "elements[$index]")
        }
        return CreateRows(schema, rows)
    }
}

data class CreateConfig(val elements: List<JsonNode> = emptyList())

private class CreateRows(private val schema: Schema, private val rows: List<Row>) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> = begin.apply(Create.of(rows).withRowSchema(schema))

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
