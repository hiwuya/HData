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
 * Expands an ARRAY/ITERABLE field into one output row per element.
 *
 * ```yaml
 * - type: Explode
 *   config: { field: tags, output_field: tag }
 * ```
 *
 * When `output_field` is omitted or equals `field`, the collection field is replaced by its element.
 * Null and empty collections produce no rows, matching inner `UNNEST` semantics.
 */
class ExplodeProvider : TypedTransformProvider<ExplodeConfig>(ExplodeConfig::class.java) {

    override fun identifier(): String = "Explode"

    override fun description(): String = "Expands an ARRAY or ITERABLE field into one row per element"

    override fun create(config: ExplodeConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        require(config.field.isNotBlank()) { "Explode requires a non-blank field" }
        require(config.outputField.isBlank() || config.outputField.none(Char::isWhitespace)) {
            "Explode's output_field must be blank or a field name without whitespace"
        }
        return ExplodeRows(config.field, config.outputField.ifBlank { config.field })
    }
}

data class ExplodeConfig(
    val field: String = "",
    val outputField: String = "",
)

private class ExplodeRows(
    private val field: String,
    private val outputField: String,
) : RowTransform() {

    override fun transform(input: PCollection<Row>): PCollection<Row> {
        val schema = input.schema
        if (field !in schema.fieldNames) {
            throw HDataException("Explode references non-existent field [$field], input fields: ${schema.fieldNames}")
        }
        val inputField = schema.getField(field)
        if (inputField.type.typeName !in setOf(Schema.TypeName.ARRAY, Schema.TypeName.ITERABLE)) {
            throw HDataException("Explode field [$field] must be ARRAY or ITERABLE, but was ${inputField.type.typeName}")
        }
        if (outputField != field && outputField in schema.fieldNames) {
            throw HDataException("Explode output_field [$outputField] already exists; choose another name or replace [$field]")
        }
        val elementType = inputField.type.collectionElementType!!
        val outputSchema = Schema.builder().apply {
            schema.fields.forEach { existing ->
                if (existing.name == field) {
                    if (outputField == field) addField(existing.withType(elementType)) else addField(existing)
                } else addField(existing)
            }
            if (outputField != field) addField(Schema.Field.of(outputField, elementType))
        }.build()
        return input.apply(ParDo.of(ExplodeFn(schema.indexOf(field), outputField == field, outputSchema))).setRowSchema(outputSchema)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

private class ExplodeFn(
    private val fieldIndex: Int,
    private val replacesField: Boolean,
    private val outputSchema: Schema,
) : DoFn<Row, Row>() {

    @ProcessElement
    fun processElement(@Element row: Row, receiver: OutputReceiver<Row>) {
        val elements = row.getValue<Any?>(fieldIndex) as? Iterable<*> ?: return
        for (element in elements) {
            val values = row.values.toMutableList()
            if (replacesField) values[fieldIndex] = element else values += element
            receiver.output(Row.withSchema(outputSchema).addValues(values).build())
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
