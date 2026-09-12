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

/** Replaces null values with typed literals while retaining the input schema. */
class FillNullsProvider : TypedTransformProvider<FillNullsConfig>(FillNullsConfig::class.java) {
    override fun identifier(): String = "FillNulls"
    override fun description(): String = "Replaces null field values with typed literals"

    override fun create(config: FillNullsConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        require(config.fields.isNotEmpty()) { "FillNulls requires at least one field" }
        require(config.fields.keys.none(String::isBlank)) { "FillNulls field names must not be blank" }
        require(config.fields.values.none { it.isNull }) { "FillNulls defaults must not be null" }
        return FillNulls(SpecMappers.CONFIG.writeValueAsString(config.fields))
    }
}

data class FillNullsConfig(val fields: Map<String, JsonNode> = emptyMap())

private class FillNulls(private val fieldsJson: String) : RowTransform() {
    override fun transform(input: PCollection<Row>): PCollection<Row> {
        val defaults = SpecMappers.CONFIG.readTree(fieldsJson)
        val names = defaults.propertyNames().toList()
        val unknown = names - input.schema.fieldNames.toSet()
        if (unknown.isNotEmpty()) {
            throw HDataException("FillNulls references non-existent fields $unknown, input fields: ${input.schema.fieldNames}")
        }
        val nullableSchema = Schema.builder().apply {
            input.schema.fields.forEach { addField(it.withType(it.type.withNullable(true))) }
        }.build()
        val converted = RowConverters.toRow(nullableSchema, defaults, "FillNulls.fields")
        val values = names.associate { input.schema.indexOf(it) to converted.getValue<Any?>(it) }
        return input.apply(ParDo.of(FillNullsFn(values))).setRowSchema(input.schema)
    }
}

private class FillNullsFn(private val defaults: Map<Int, Any?>) : DoFn<Row, Row>() {
    @ProcessElement fun processElement(@Element row: Row, receiver: OutputReceiver<Row>) {
        val values = row.values.toMutableList()
        defaults.forEach { (index, default) -> if (values[index] == null) values[index] = default }
        receiver.output(Row.withSchema(row.schema).addValues(values).build())
    }
}
