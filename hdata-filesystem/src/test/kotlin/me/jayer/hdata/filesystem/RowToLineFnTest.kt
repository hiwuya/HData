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
 * `RowToLineFn`：把 [Row] 转成要写出的一行文本。
 *
 * 重构前 CSVFormat 是写死的，配置里的 `csv_delimiter` 根本没传下去；且 text 模式下
 * `content` 缺失 / 为 null 的情况没人管。这里证明配置真的生效、异常真的进死信。
 *
 * @author wuya
 */
class RowToLineFnTest {

    private val textSchema = FilesystemSchemas.TEXT_SCHEMA
    private val errorSchema = ErrorSchemas.of(textSchema)
    private val errorTag = TupleTag<Row>("errors")

    private fun tester(fn: RowToLineFn): DoFnTester<Row, String> = DoFnTester.of(fn)

    @Test
    fun `text 模式把 content 字段写成一行`() {
        val fn = RowToLineFn(FilesystemWriteConfig(path = "/o"), errorSchema, false, "w", errorTag)
        val out = tester(fn).apply { processElement(Row.withSchema(textSchema).addValue("hello").build()) }
            .takeOutputElements()
        assertEquals(listOf("hello"), out)
    }

    @Test
    fun `text 模式缺 content 字段直接报错`() {
        val schema = Schema.builder().addStringField("other").build()
        val fn = RowToLineFn(FilesystemWriteConfig(path = "/o"), ErrorSchemas.of(schema), false, "w", errorTag)
        assertFailsWith<IllegalArgumentException> {
            tester(fn).processElement(Row.withSchema(schema).addValue("x").build())
        }
    }

    @Test
    fun `text 模式 content 为 null 进死信`() {
        // 上游若产出可空 content 且值为 null，这里必须进死信而不是写出空行
        val nullableText = Schema.builder().addNullableStringField("content").build()
        val fn = RowToLineFn(FilesystemWriteConfig(path = "/o"), ErrorSchemas.of(nullableText), true, "w", errorTag)
        val t = tester(fn)
        t.processElement(Row.withSchema(nullableText).addValue(null).build())
        assertTrue(t.takeOutputElements().isEmpty())
        assertTrue(t.peekOutputElements(errorTag).iterator().hasNext())
    }

    @Test
    fun `csv 模式真的用配置的 csv_delimiter`() {
        // 重构前 CSVFormat.DEFAULT 写死逗号，delimiter 没传下去
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
}
