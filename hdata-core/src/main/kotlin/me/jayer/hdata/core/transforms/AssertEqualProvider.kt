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
 * 断言输入恰好等于给定的记录集合（不计顺序），原样透传，用于给 pipeline 文件写测试：
 *
 * ```yaml
 * - type: AssertEqual
 *   config:
 *     elements:
 *       - { id: 1, name: "a" }
 * ```
 *
 * 断言由 runner 执行，需要 DirectRunner 这类支持断言的 runner。
 *
 * @author wuya
 * @date 2022-08-30
 */
class AssertEqualProvider : TypedTransformProvider<AssertEqualConfig>(AssertEqualConfig::class.java) {

    override fun identifier(): String = "AssertEqual"

    override fun description(): String = "断言输入等于给定记录集合"

    override fun create(config: AssertEqualConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> =
        // 期望值的 schema 要等到 expand 时才知道，这里先按 JSON 文本保存，避免把语法树塞进 PTransform
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
