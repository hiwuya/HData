package me.jayer.hdata.core.error

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row

/**
 * The schema convention for dead-letter records.
 *
 * For an input schema `S`, the dead-letter schema is fixed as:
 *
 * ```
 * element       ROW<S>   NULLABLE
 * error_type    STRING
 * error_message STRING   NULLABLE
 * transform     STRING
 * ```
 *
 * Keeping the original record (instead of encoding it into a string) means the dead-letter stream
 * is still a schema-bearing `PCollection<Row>`, which can be fed directly into a sink for storage,
 * or restored back to the original record with `StripErrorMetadata` and replayed.
 *
 * @author wuya
 * @date 2022-08-30
 */
object ErrorSchemas {

    const val ELEMENT = "element"
    const val ERROR_TYPE = "error_type"
    const val ERROR_MESSAGE = "error_message"
    const val TRANSFORM = "transform"

    fun of(elementSchema: Schema): Schema = Schema.builder()
        .addNullableField(ELEMENT, Schema.FieldType.row(elementSchema))
        .addStringField(ERROR_TYPE)
        .addNullableStringField(ERROR_MESSAGE)
        .addStringField(TRANSFORM)
        .build()

    /** Whether [schema] is a dead-letter schema produced by this convention. */
    fun isErrorSchema(schema: Schema): Boolean {
        if (schema.fieldNames != listOf(ELEMENT, ERROR_TYPE, ERROR_MESSAGE, TRANSFORM)) return false
        val elementType = schema.getField(ELEMENT).type
        return elementType.typeName == Schema.TypeName.ROW &&
            elementType.nullable &&
            schema.getField(ERROR_TYPE).type == Schema.FieldType.STRING &&
            schema.getField(ERROR_MESSAGE).type == Schema.FieldType.STRING.withNullable(true) &&
            schema.getField(TRANSFORM).type == Schema.FieldType.STRING
    }

    fun failure(errorSchema: Schema, element: Row?, error: Throwable, transform: String): Row =
        Row.withSchema(errorSchema)
            .addValue(element)
            .addValue(error.javaClass.name)
            .addValue(error.message)
            .addValue(transform)
            .build()
}
