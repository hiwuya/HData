package me.jayer.hdata.core.error

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * @author wuya
 * @date 2022-08-30
 */
class ErrorSchemasTest {

    private val elementSchema = Schema.builder()
        .addInt64Field("id")
        .addNullableStringField("name")
        .build()

    @Test
    fun `死信 schema 保留原始记录的结构`() {
        val schema = ErrorSchemas.of(elementSchema)

        assertEquals(
            listOf(ErrorSchemas.ELEMENT, ErrorSchemas.ERROR_TYPE, ErrorSchemas.ERROR_MESSAGE, ErrorSchemas.TRANSFORM),
            schema.fieldNames,
        )
        assertEquals(elementSchema, schema.getField(ErrorSchemas.ELEMENT).type.rowSchema)
        assertTrue(schema.getField(ErrorSchemas.ELEMENT).type.nullable)
        assertTrue(schema.getField(ErrorSchemas.ERROR_MESSAGE).type.nullable)
        assertFalse(schema.getField(ErrorSchemas.ERROR_TYPE).type.nullable)
    }

    @Test
    fun `failure 把异常信息与原始记录装进一行`() {
        val schema = ErrorSchemas.of(elementSchema)
        val element = Row.withSchema(elementSchema).addValues(1L, "a").build()

        val failure = ErrorSchemas.failure(schema, element, IllegalStateException("boom"), "WriteToJdbc")

        assertEquals(element, failure.getRow(ErrorSchemas.ELEMENT))
        assertEquals(IllegalStateException::class.java.name, failure.getString(ErrorSchemas.ERROR_TYPE))
        assertEquals("boom", failure.getString(ErrorSchemas.ERROR_MESSAGE))
        assertEquals("WriteToJdbc", failure.getString(ErrorSchemas.TRANSFORM))
    }

    @Test
    fun `原始记录与异常信息都允许为空`() {
        val schema = ErrorSchemas.of(elementSchema)

        val failure = ErrorSchemas.failure(schema, null, IllegalStateException(), "Sink")

        assertNull(failure.getRow(ErrorSchemas.ELEMENT))
        assertNull(failure.getString(ErrorSchemas.ERROR_MESSAGE))
    }

    @Test
    fun `isErrorSchema 只认本约定产出的 schema`() {
        assertTrue(ErrorSchemas.isErrorSchema(ErrorSchemas.of(elementSchema)))
        assertFalse(ErrorSchemas.isErrorSchema(elementSchema))
        // 字段名对上但 element 不是 ROW，也不算
        val lookalike = Schema.builder()
            .addNullableStringField(ErrorSchemas.ELEMENT)
            .addStringField(ErrorSchemas.ERROR_TYPE)
            .addStringField(ErrorSchemas.TRANSFORM)
            .build()
        assertFalse(ErrorSchemas.isErrorSchema(lookalike))

        // 少字段或字段类型错误的相似 schema 也不能被 StripErrorMetadata 当成死信流
        val missingMessage = Schema.builder()
            .addNullableField(ErrorSchemas.ELEMENT, Schema.FieldType.row(elementSchema))
            .addStringField(ErrorSchemas.ERROR_TYPE)
            .addStringField(ErrorSchemas.TRANSFORM)
            .build()
        assertFalse(ErrorSchemas.isErrorSchema(missingMessage))

        val wrongMessageType = Schema.builder()
            .addNullableField(ErrorSchemas.ELEMENT, Schema.FieldType.row(elementSchema))
            .addStringField(ErrorSchemas.ERROR_TYPE)
            .addInt32Field(ErrorSchemas.ERROR_MESSAGE)
            .addStringField(ErrorSchemas.TRANSFORM)
            .build()
        assertFalse(ErrorSchemas.isErrorSchema(wrongMessageType))
    }
}
