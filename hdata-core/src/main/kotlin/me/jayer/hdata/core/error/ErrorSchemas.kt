package me.jayer.hdata.core.error

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row

/**
 * 死信记录的 schema 约定。
 *
 * 对某个输入 schema `S`，死信 schema 固定为：
 *
 * ```
 * element       ROW<S>   NULLABLE
 * error_type    STRING
 * error_message STRING   NULLABLE
 * transform     STRING
 * ```
 *
 * 保留原始记录（而不是把它编码成字符串）意味着死信流依旧是带 schema 的 `PCollection<Row>`，
 * 可以直接接一个 sink 落库，或者用 `StripErrorMetadata` 还原成原始记录后重放。
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

    /** [schema] 是否是本约定产出的死信 schema。 */
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
