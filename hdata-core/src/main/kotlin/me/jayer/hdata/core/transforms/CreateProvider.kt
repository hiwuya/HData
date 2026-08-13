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
 * 用字面量造数据，schema 由字面量推断。测试与联调用：
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

    override fun description(): String = "从配置里的字面量构造一个有限数据集"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(config: CreateConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        require(config.elements.isNotEmpty()) { "Create 至少需要一条 elements" }
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
