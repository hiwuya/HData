package me.jayer.hdata.core.transforms

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spi.RowTransform
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * 字段选择 / 改名 / 丢弃，是数据同步里最常用的一步：
 *
 * ```yaml
 * - type: MapToFields
 *   config:
 *     fields:
 *       order_id: c_id      # 目标字段: 源字段
 *       amount: c_amount
 * ```
 *
 * `append: true` 时保留输入的全部字段再叠加 `fields`，配合 `drop` 做减法。
 * 这里只支持纯字段引用，不引入表达式语言——真正的计算交给 SQL 或专门的 transform。
 *
 * @author wuya
 * @date 2022-08-30
 */
class MapToFieldsProvider : TypedTransformProvider<MapToFieldsConfig>(MapToFieldsConfig::class.java) {

    override fun identifier(): String = "MapToFields"

    override fun description(): String = "按字段引用做选择、改名与丢弃"

    override fun create(config: MapToFieldsConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        require(config.fields.isNotEmpty() || (config.append && config.drop.isNotEmpty())) {
            "MapToFields 需要声明 fields，或者在 append: true 时声明 drop"
        }
        require(!(config.drop.isNotEmpty() && !config.append)) {
            "MapToFields 的 drop 只在 append: true 时有意义，否则请直接在 fields 里列出要保留的字段"
        }
        return MapToFields(config.fields, config.append, config.drop)
    }
}

data class MapToFieldsConfig(
    /** 目标字段名 -> 源字段名。 */
    val fields: Map<String, String> = emptyMap(),
    val append: Boolean = false,
    val drop: List<String> = emptyList(),
)

private class MapToFields(
    private val fields: Map<String, String>,
    private val append: Boolean,
    private val drop: List<String>,
) : RowTransform() {

    override fun transform(input: PCollection<Row>): PCollection<Row> {
        val inputSchema = input.schema
        val unknownDrop = drop - inputSchema.fieldNames.toSet()
        if (unknownDrop.isNotEmpty()) {
            throw HDataException("MapToFields 的 drop 引用了不存在的字段 $unknownDrop，输入字段: ${inputSchema.fieldNames}")
        }

        // 目标字段名 -> 源字段名，后写的覆盖 append 带进来的同名字段
        val mapping = linkedMapOf<String, String>()
        if (append) {
            inputSchema.fieldNames.filterNot { it in drop }.forEach { mapping[it] = it }
        }
        mapping.putAll(fields)

        val unknownSources = mapping.values.toSet() - inputSchema.fieldNames.toSet()
        if (unknownSources.isNotEmpty()) {
            throw HDataException("MapToFields 引用了不存在的字段 $unknownSources，输入字段: ${inputSchema.fieldNames}")
        }

        val outputSchema = Schema.builder()
            .addFields(mapping.map { (target, source) ->
                inputSchema.getField(source).withName(target)
            })
            .build()
        val indexes = mapping.values.map { inputSchema.indexOf(it) }
        return input.apply(ParDo.of(MapToFieldsFn(outputSchema, indexes))).setRowSchema(outputSchema)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

private class MapToFieldsFn(
    private val outputSchema: Schema,
    private val indexes: List<Int>,
) : DoFn<Row, Row>() {

    @ProcessElement
    fun processElement(@Element row: Row, receiver: OutputReceiver<Row>) {
        val builder = Row.withSchema(outputSchema)
        indexes.forEach { builder.addValue(row.getValue<Any?>(it)) }
        receiver.output(builder.build())
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
