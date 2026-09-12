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
import tools.jackson.databind.node.JsonNodeFactory

/**
 * Keeps rows whose one field matches a typed literal predicate.
 *
 * ```yaml
 * - type: Filter
 *   config: { field: op, operator: in, values: [c, r, u] }
 * ```
 */
class FilterProvider : TypedTransformProvider<FilterConfig>(FilterConfig::class.java) {

    override fun identifier(): String = "Filter"

    override fun description(): String = "Filters rows with a typed field predicate"

    override fun create(config: FilterConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        require(config.field.isNotBlank()) { "Filter requires a non-blank field" }
        val operator = FilterOperator.parse(config.operator)
        when (operator) {
            FilterOperator.EQUALS, FilterOperator.NOT_EQUALS -> require(config.value != null) {
                "Filter operator [${operator.configName}] requires value"
            }
            FilterOperator.IN -> require(config.values.isNotEmpty()) { "Filter operator [in] requires a non-empty values list" }
            FilterOperator.IS_NULL, FilterOperator.IS_NOT_NULL -> require(config.value == null && config.values.isEmpty()) {
                "Filter operator [${operator.configName}] does not accept value or values"
            }
        }
        return FilterRows(config.field, operator, SpecMappers.CONFIG.writeValueAsString(config.value),
            SpecMappers.CONFIG.writeValueAsString(config.values))
    }
}

data class FilterConfig(
    val field: String = "",
    val operator: String = "equals",
    val value: JsonNode? = null,
    val values: List<JsonNode> = emptyList(),
)

private enum class FilterOperator(val configName: String) {
    EQUALS("equals"), NOT_EQUALS("not_equals"), IN("in"), IS_NULL("is_null"), IS_NOT_NULL("is_not_null");

    companion object {
        fun parse(value: String): FilterOperator = entries.firstOrNull { it.configName == value.lowercase() }
            ?: throw IllegalArgumentException("Filter's operator must be ${entries.joinToString("/") { it.configName }}, but was: $value")
    }
}

private class FilterRows(
    private val field: String,
    private val operator: FilterOperator,
    private val valueJson: String,
    private val valuesJson: String,
) : RowTransform() {

    override fun transform(input: PCollection<Row>): PCollection<Row> {
        if (field !in input.schema.fieldNames) {
            throw HDataException("Filter references non-existent field [$field], input fields: ${input.schema.fieldNames}")
        }
        val fieldType = input.schema.getField(field).type
        val valueNode: JsonNode = SpecMappers.CONFIG.readTree(valueJson)
        val value = if (valueNode.isNull) null else convert(valueNode, fieldType)
        val valueNodes: List<JsonNode> = SpecMappers.CONFIG.readValue(valuesJson, VALUES_TYPE)
        val values: List<Any?> = valueNodes.map { convert(it, fieldType) }
        return input.apply(ParDo.of(FilterFn(input.schema.indexOf(field), operator, value, values))).setRowSchema(input.schema)
    }

    private fun convert(value: JsonNode, fieldType: Schema.FieldType): Any? {
        val schema = Schema.builder().addField("value", fieldType).build()
        val node = JsonNodeFactory.instance.objectNode().set("value", value)
        return RowConverters.toRow(schema, node, "Filter.value").getValue(0)
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val VALUES_TYPE = SpecMappers.CONFIG.typeFactory
            .constructCollectionType(List::class.java, JsonNode::class.java)
    }
}

private class FilterFn(
    private val fieldIndex: Int,
    private val operator: FilterOperator,
    private val value: Any?,
    private val values: List<Any?>,
) : DoFn<Row, Row>() {

    @ProcessElement
    fun processElement(@Element row: Row, receiver: OutputReceiver<Row>) {
        val actual = row.getValue<Any?>(fieldIndex)
        val matches = when (operator) {
            FilterOperator.EQUALS -> same(actual, value)
            FilterOperator.NOT_EQUALS -> !same(actual, value)
            FilterOperator.IN -> values.any { same(actual, it) }
            FilterOperator.IS_NULL -> actual == null
            FilterOperator.IS_NOT_NULL -> actual != null
        }
        if (matches) receiver.output(row)
    }

    private fun same(left: Any?, right: Any?): Boolean = when {
        left is ByteArray && right is ByteArray -> left.contentEquals(right)
        else -> left == right
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
