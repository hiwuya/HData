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
    fun `the dead-letter schema preserves the structure of the original record`() {
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
    fun `failure packs the exception info and the original record into one row`() {
        val schema = ErrorSchemas.of(elementSchema)
        val element = Row.withSchema(elementSchema).addValues(1L, "a").build()

        val failure = ErrorSchemas.failure(schema, element, IllegalStateException("boom"), "WriteToJdbc")

        assertEquals(element, failure.getRow(ErrorSchemas.ELEMENT))
        assertEquals(IllegalStateException::class.java.name, failure.getString(ErrorSchemas.ERROR_TYPE))
        assertEquals("boom", failure.getString(ErrorSchemas.ERROR_MESSAGE))
        assertEquals("WriteToJdbc", failure.getString(ErrorSchemas.TRANSFORM))
    }

    @Test
    fun `both the original record and the exception info are allowed to be null`() {
        val schema = ErrorSchemas.of(elementSchema)

        val failure = ErrorSchemas.failure(schema, null, IllegalStateException(), "Sink")

        assertNull(failure.getRow(ErrorSchemas.ELEMENT))
        assertNull(failure.getString(ErrorSchemas.ERROR_MESSAGE))
    }

    @Test
    fun `isErrorSchema only recognizes schemas produced by this convention`() {
        assertTrue(ErrorSchemas.isErrorSchema(ErrorSchemas.of(elementSchema)))
        assertFalse(ErrorSchemas.isErrorSchema(elementSchema))
        // field names match but element is not a ROW, so it does not count either
        val lookalike = Schema.builder()
            .addNullableStringField(ErrorSchemas.ELEMENT)
            .addStringField(ErrorSchemas.ERROR_TYPE)
            .addStringField(ErrorSchemas.TRANSFORM)
            .build()
        assertFalse(ErrorSchemas.isErrorSchema(lookalike))

        // a similar schema missing a field or with a wrong field type must not be treated by StripErrorMetadata as a dead-letter stream either
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
