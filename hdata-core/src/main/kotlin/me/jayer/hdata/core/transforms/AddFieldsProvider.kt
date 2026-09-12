package me.jayer.hdata.core.transforms

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.RowTransform
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.core.util.RowConverters
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import tools.jackson.databind.JsonNode
import tools.jackson.databind.node.ObjectNode

/**
 * Appends constant fields to every row. This is useful for static partition values, lineage labels,
 * and normalizing CDC records before they enter a shared sink.
 *
 * ```yaml
 * - type: AddFields
 *   config:
 *     fields:
 *       source_system: orders
 *       active: true
 * ```
 */
class AddFieldsProvider : TypedTransformProvider<AddFieldsConfig>(AddFieldsConfig::class.java) {

    override fun identifier(): String = "AddFields"

    override fun description(): String = "Appends constant fields to every input row"

    override fun create(config: AddFieldsConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        require(config.fields.isNotEmpty()) { "AddFields requires at least one field" }
        require(config.fields.keys.none(String::isBlank)) { "AddFields' field names must not be blank" }
        return AddFields(SpecMappers.CONFIG.writeValueAsString(config.fields))
    }
}

data class AddFieldsConfig(val fields: Map<String, JsonNode> = emptyMap())

private class AddFields(private val fieldsJson: String) : RowTransform() {

    override fun transform(input: PCollection<Row>): PCollection<Row> {
        val fields: LinkedHashMap<String, JsonNode> = SpecMappers.CONFIG.readValue(fieldsJson, FIELDS_TYPE)
        val duplicate = fields.keys.intersect(input.schema.fieldNames.toSet())
        if (duplicate.isNotEmpty()) {
            throw HDataException("AddFields cannot overwrite existing fields: ${duplicate.sorted().joinToString(", ")}")
        }
        val literals = SpecMappers.CONFIG.valueToTree<ObjectNode>(fields)
        val literalSchema = RowConverters.inferSchema(listOf(literals), "fields")
        val literalRow = RowConverters.toRow(literalSchema, literals, "fields")
        val outputSchema = Schema.builder().addFields(input.schema.fields).addFields(literalSchema.fields).build()
        return input.apply(ParDo.of(AddFieldsFn(outputSchema, literalRow))).setRowSchema(outputSchema)
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val FIELDS_TYPE = SpecMappers.CONFIG.typeFactory
            .constructMapType(LinkedHashMap::class.java, String::class.java, JsonNode::class.java)
    }
}

private class AddFieldsFn(
    private val outputSchema: Schema,
    private val literalRow: Row,
) : DoFn<Row, Row>() {

    @ProcessElement
    fun processElement(@Element row: Row, receiver: OutputReceiver<Row>) {
        receiver.output(Row.withSchema(outputSchema).addValues(row.values).addValues(literalRow.values).build())
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
