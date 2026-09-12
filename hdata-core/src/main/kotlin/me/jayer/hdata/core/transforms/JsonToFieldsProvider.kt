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

/** Parses a JSON STRING field into explicitly typed top-level fields. */
class JsonToFieldsProvider : TypedTransformProvider<JsonToFieldsConfig>(JsonToFieldsConfig::class.java) {
    override fun identifier(): String = "JsonToFields"
    override fun description(): String = "Parses a JSON string field into typed top-level fields"

    override fun create(config: JsonToFieldsConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        require(config.field.isNotBlank()) { "JsonToFields requires a non-blank field" }
        require(config.fields.isNotEmpty()) { "JsonToFields requires at least one fields entry" }
        val fields = config.fields.map(::parseField)
        require(fields.map { it.name }.distinct().size == fields.size) { "JsonToFields fields must not contain duplicate names" }
        return JsonToFieldsRows(config.field, fields, config.dropInput)
    }

    private fun parseField(spec: String): Schema.Field {
        val parts = spec.split(':', limit = 2)
        require(parts.size == 2 && parts[0].isNotBlank() && parts[1].isNotBlank()) {
            "JsonToFields fields entries must be name:type, received: $spec"
        }
        val type = when (parts[1].lowercase()) {
            "string" -> Schema.FieldType.STRING
            "boolean", "bool" -> Schema.FieldType.BOOLEAN
            "byte", "int8" -> Schema.FieldType.BYTE
            "short", "int16" -> Schema.FieldType.INT16
            "int", "int32" -> Schema.FieldType.INT32
            "long", "int64" -> Schema.FieldType.INT64
            "float" -> Schema.FieldType.FLOAT
            "double" -> Schema.FieldType.DOUBLE
            "decimal" -> Schema.FieldType.DECIMAL
            "bytes" -> Schema.FieldType.BYTES
            else -> throw IllegalArgumentException("JsonToFields unsupported type: ${parts[1]}")
        }
        return Schema.Field.of(parts[0], type).withNullable(true)
    }
}

data class JsonToFieldsConfig(val field: String = "", val fields: List<String> = emptyList(), val dropInput: Boolean = false)

private class JsonToFieldsRows(
    private val field: String,
    private val fields: List<Schema.Field>,
    private val dropInput: Boolean,
) : RowTransform() {
    override fun transform(input: PCollection<Row>): PCollection<Row> {
        if (field !in input.schema.fieldNames) throw HDataException("JsonToFields references non-existent field [$field]")
        if (input.schema.getField(field).type.typeName != Schema.TypeName.STRING) {
            throw HDataException("JsonToFields field [$field] must be STRING")
        }
        val retainedFields = input.schema.fieldNames.toSet().let { names ->
            if (dropInput) names - field else names
        }
        val collisions = fields.map { it.name }.intersect(retainedFields)
        if (collisions.isNotEmpty()) throw HDataException("JsonToFields output fields already exist: ${collisions.sorted().joinToString(", ")}")
        val output = Schema.builder().apply {
            input.schema.fields.filterNot { dropInput && it.name == field }.forEach(::addField)
            addFields(fields)
        }.build()
        return input.apply(ParDo.of(JsonToFieldsFn(input.schema.indexOf(field), dropInput, fields, output))).setRowSchema(output)
    }
}

private class JsonToFieldsFn(
    private val jsonIndex: Int,
    private val dropInput: Boolean,
    private val fields: List<Schema.Field>,
    private val outputSchema: Schema,
) : DoFn<Row, Row>() {
    @Transient private var mapper: tools.jackson.databind.ObjectMapper? = null

    @Setup fun setup() { mapper = SpecMappers.CONFIG }

    @ProcessElement fun processElement(@Element row: Row, receiver: OutputReceiver<Row>) {
        val json = row.getValue<String?>(jsonIndex)
        val node = if (json == null) null else try { mapper!!.readTree(json) } catch (e: Exception) {
            throw HDataException("JsonToFields cannot parse field as JSON: ${e.message}", e)
        }
        if (node != null && !node.isObject) throw HDataException("JsonToFields input must contain a JSON object")
        val values = row.values.toMutableList().also { if (dropInput) it.removeAt(jsonIndex) }
        fields.forEach { field -> values += convert(field, node?.get(field.name)) }
        receiver.output(Row.withSchema(outputSchema).addValues(values).build())
    }

    private fun convert(field: Schema.Field, value: JsonNode?): Any? {
        val one = Schema.builder().addField(field).build()
        val node = JsonNodeFactory.instance.objectNode().also { if (value != null) it.set(field.name, value) }
        return RowConverters.toRow(one, node, "JsonToFields.${field.name}").getValue(0)
    }
}
