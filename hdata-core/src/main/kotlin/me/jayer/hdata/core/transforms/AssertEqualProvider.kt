package me.jayer.hdata.core.transforms

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.RowTransform
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.core.util.RowConverters
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import tools.jackson.databind.JsonNode

/**
 * Asserts that the input equals exactly the given set of records (order-insensitive) and passes them through unchanged, used to write tests for pipeline files:
 *
 * ```yaml
 * - type: AssertEqual
 *   config:
 *     elements:
 *       - { id: 1, name: "a" }
 * ```
 *
 * Assertions are executed by the runner, so a runner that supports assertions such as DirectRunner is required.
 *
 * @author wuya
 * @date 2022-08-30
 */
class AssertEqualProvider : TypedTransformProvider<AssertEqualConfig>(AssertEqualConfig::class.java) {

    override fun identifier(): String = "AssertEqual"

    override fun description(): String = "Asserts that the input equals the given set of records"

    override fun create(config: AssertEqualConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> =
        // The expected values' schema is not known until expand, so we store it as JSON text here to avoid stuffing the syntax tree into the PTransform
        AssertEqual(SpecMappers.CONFIG.writeValueAsString(config.elements))
}

data class AssertEqualConfig(val elements: List<JsonNode> = emptyList())

private class AssertEqual(private val elementsJson: String) : RowTransform() {

    override fun transform(input: PCollection<Row>): PCollection<Row> {
        val schema = input.schema
        val elements: List<JsonNode> = SpecMappers.CONFIG.readValue(elementsJson, ELEMENTS_TYPE)
        val expected = elements.mapIndexed { index, element ->
            RowConverters.toRow(schema, element, "elements[$index]")
        }
        PAssert.that(input).containsInAnyOrder(expected)
        return input
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val ELEMENTS_TYPE = SpecMappers.CONFIG.typeFactory
            .constructCollectionType(List::class.java, JsonNode::class.java)
    }
}
