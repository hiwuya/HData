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
 * Field selection / renaming / dropping, the most commonly used step in data synchronization:
 *
 * ```yaml
 * - type: MapToFields
 *   config:
 *     fields:
 *       order_id: c_id      # target field: source field
 *       amount: c_amount
 * ```
 *
 * With `append: true`, all input fields are kept and then `fields` is layered on top, working together with `drop` for subtraction.
 * Only plain field references are supported here; no expression language is introduced — real computation is left to SQL or a dedicated transform.
 *
 * @author wuya
 * @date 2022-08-30
 */
class MapToFieldsProvider : TypedTransformProvider<MapToFieldsConfig>(MapToFieldsConfig::class.java) {

    override fun identifier(): String = "MapToFields"

    override fun description(): String = "Selects, renames and drops fields by field reference"

    override fun create(config: MapToFieldsConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        require(config.fields.isNotEmpty() || (config.append && config.drop.isNotEmpty())) {
            "MapToFields requires fields to be declared, or drop to be declared when append: true"
        }
        require(config.fields.keys.none { it.isBlank() }) { "MapToFields' target field name must not be empty" }
        require(config.fields.values.none { it.isBlank() }) { "MapToFields' source field name must not be empty" }
        require(config.drop.none { it.isBlank() }) { "MapToFields' drop must not contain an empty field name" }
        require(config.drop.distinct().size == config.drop.size) { "MapToFields' drop must not contain duplicates" }
        require(!(config.drop.isNotEmpty() && !config.append)) {
            "MapToFields' drop only makes sense when append: true; otherwise list the fields to keep directly in fields"
        }
        return MapToFields(config.fields, config.append, config.drop)
    }
}

data class MapToFieldsConfig(
    /** Target field name -> source field name. */
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
            throw HDataException("MapToFields' drop references non-existent fields $unknownDrop, input fields: ${inputSchema.fieldNames}")
        }

        // Target field name -> source field name; later entries override same-named fields brought in by append
        val mapping = linkedMapOf<String, String>()
        if (append) {
            inputSchema.fieldNames.filterNot { it in drop }.forEach { mapping[it] = it }
        }
        mapping.putAll(fields)

        val unknownSources = mapping.values.toSet() - inputSchema.fieldNames.toSet()
        if (unknownSources.isNotEmpty()) {
            throw HDataException("MapToFields references non-existent fields $unknownSources, input fields: ${inputSchema.fieldNames}")
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
