package me.jayer.hdata.filesystem

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.filesystem.transform.RowToLineFn
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFnTester
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.TupleTag
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * `RowToLineFn`: turns a [Row] into the line of text to write out.
 *
 * Before the refactor the CSVFormat was hardcoded and the configured `csv_delimiter` was never
 * passed down; also the missing / null `content` case in text mode was unhandled. These tests prove
 * that the config really takes effect and that failures really go to the dead letter.
 *
 * @author wuya
 */
class RowToLineFnTest {

    private val textSchema = FilesystemSchemas.TEXT_SCHEMA
    private val errorSchema = ErrorSchemas.of(textSchema)
    private val errorTag = TupleTag<Row>("errors")

    private fun tester(fn: RowToLineFn): DoFnTester<Row, String> = DoFnTester.of(fn)

    @Test
    fun `text mode writes the content field as one line`() {
        val fn = RowToLineFn(FilesystemWriteConfig(path = "/o"), errorSchema, false, "w", errorTag)
        val out = tester(fn).apply { processElement(Row.withSchema(textSchema).addValue("hello").build()) }
            .takeOutputElements()
        assertEquals(listOf("hello"), out)
    }

    @Test
    fun `text mode raises an error when the content field is missing`() {
        val schema = Schema.builder().addStringField("other").build()
        val fn = RowToLineFn(FilesystemWriteConfig(path = "/o"), ErrorSchemas.of(schema), false, "w", errorTag)
        assertFailsWith<IllegalArgumentException> {
            tester(fn).processElement(Row.withSchema(schema).addValue("x").build())
        }
    }

    @Test
    fun `text mode sends a null content to the dead letter`() {
        // if upstream produces a nullable content that is null, it must go to the dead letter instead of writing an empty line
        val nullableText = Schema.builder().addNullableStringField("content").build()
        val fn = RowToLineFn(FilesystemWriteConfig(path = "/o"), ErrorSchemas.of(nullableText), true, "w", errorTag)
        val t = tester(fn)
        t.processElement(Row.withSchema(nullableText).addValue(null).build())
        assertTrue(t.takeOutputElements().isEmpty())
        assertTrue(t.peekOutputElements(errorTag).iterator().hasNext())
    }

    @Test
    fun `csv mode really uses the configured csv_delimiter`() {
        // before the refactor CSVFormat.DEFAULT hardcoded a comma and the delimiter was never passed down
        val schema = Schema.builder().addNullableStringField("name").addNullableInt32Field("age").build()
        val fn = RowToLineFn(
            FilesystemWriteConfig(
                path = "/o",
                fileFormat = "csv",
                schemaFields = listOf("name:string", "age:int"),
                csvDelimiter = "|",
            ),
            ErrorSchemas.of(schema),
            false,
            "w",
            errorTag,
        )
        val out = tester(fn).apply { processElement(Row.withSchema(schema).addValue("张三").addValue(30).build()) }
            .takeOutputElements()
        assertEquals("张三|30", out.single())
    }

    @Test
    fun `csv projects and reorders by schema_fields instead of input position`() {
        val inputSchema = Schema.builder()
            .addNullableInt32Field("age")
            .addNullableStringField("name")
            .addNullableStringField("ignored")
            .build()
        val fn = RowToLineFn(
            FilesystemWriteConfig(
                path = "/o",
                fileFormat = "csv",
                schemaFields = listOf("name:string", "age:int"),
            ),
            ErrorSchemas.of(inputSchema),
            false,
            "w",
            errorTag,
        )
        val row = Row.withSchema(inputSchema).addValues(30, "张三", "x").build()
        assertEquals("张三,30", tester(fn).apply { processElement(row) }.takeOutputElements().single())
    }

    @Test
    fun `csv sends rows missing a declared field to the dead letter`() {
        val inputSchema = Schema.builder().addNullableStringField("name").build()
        val fn = RowToLineFn(
            FilesystemWriteConfig(
                path = "/o",
                fileFormat = "csv",
                schemaFields = listOf("name:string", "age:int"),
            ),
            ErrorSchemas.of(inputSchema),
            true,
            "w",
            errorTag,
        )
        val t = tester(fn)
        t.processElement(Row.withSchema(inputSchema).addValue("张三").build())
        assertTrue(t.takeOutputElements().isEmpty())
        assertTrue(t.peekOutputElements(errorTag).iterator().hasNext())
    }
}
