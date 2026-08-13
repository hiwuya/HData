package me.jayer.hdata.core.util

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spec.SpecMappers
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes
import tools.jackson.databind.JsonNode
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.Base64
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * @author wuya
 * @date 2022-08-30
 */
class RowConvertersTest {

    private fun json(text: String): JsonNode = SpecMappers.CONFIG.readTree(text)

    private fun elements(vararg texts: String): List<JsonNode> = texts.map { json(it) }

    // ---------- inferSchema ----------

    @Test
    fun `整数推断为 INT64、浮点推断为 DOUBLE`() {
        val schema = RowConverters.inferSchema(elements("""{"i": 1, "d": 1.5, "b": true, "s": "x"}"""))

        assertEquals(Schema.FieldType.INT64, schema.getField("i").type)
        assertEquals(Schema.FieldType.DOUBLE, schema.getField("d").type)
        assertEquals(Schema.FieldType.BOOLEAN, schema.getField("b").type)
        assertEquals(Schema.FieldType.STRING, schema.getField("s").type)
        assertTrue(schema.fields.none { it.type.nullable })
    }

    @Test
    fun `整数与浮点混用时统一为 DOUBLE`() {
        val schema = RowConverters.inferSchema(elements("""{"v": 1}""", """{"v": 1.5}"""))
        assertEquals(Schema.FieldType.DOUBLE, schema.getField("v").type)
    }

    @Test
    fun `缺字段或显式 null 时该字段可空`() {
        val schema = RowConverters.inferSchema(elements("""{"a": 1, "b": 2}""", """{"a": 3, "b": null}""", """{"a": 4}"""))

        assertTrue(!schema.getField("a").type.nullable)
        assertTrue(schema.getField("b").type.nullable)
        // 字段顺序按首次出现
        assertEquals(listOf("a", "b"), schema.fieldNames)
    }

    @Test
    fun `全为 null 的字段退化为可空字符串`() {
        val schema = RowConverters.inferSchema(elements("""{"a": null}"""))
        assertEquals(Schema.FieldType.STRING.withNullable(true), schema.getField("a").type)
    }

    @Test
    fun `嵌套对象与数组可以推断`() {
        val schema = RowConverters.inferSchema(elements("""{"user": {"id": 1}, "tags": ["a", "b"]}"""))

        assertEquals(Schema.TypeName.ROW, schema.getField("user").type.typeName)
        assertEquals(Schema.FieldType.INT64, schema.getField("user").type.rowSchema!!.getField("id").type)
        assertEquals(Schema.FieldType.array(Schema.FieldType.STRING), schema.getField("tags").type)
    }

    @Test
    fun `同一字段类型不一致时报错`() {
        val error = assertFailsWith<HDataException> {
            RowConverters.inferSchema(elements("""{"v": 1}""", """{"v": "x"}"""))
        }
        assertTrue("类型不一致" in error.message!!)
    }

    @Test
    fun `非对象记录会报错并指出位置`() {
        val error = assertFailsWith<HDataException> {
            RowConverters.inferSchema(elements("""{"a": 1}""", """[1, 2]"""), "elements")
        }
        assertTrue("elements[1]" in error.message!!)
    }

    // ---------- toRow ----------

    @Test
    fun `按 schema 转换标量与可空字段`() {
        val schema = Schema.builder()
            .addInt32Field("i")
            .addNullableStringField("s")
            .addBooleanField("b")
            .build()

        val row = RowConverters.toRow(schema, json("""{"i": 7, "s": null, "b": true}"""))

        assertEquals(7, row.getInt32("i"))
        assertNull(row.getString("s"))
        assertEquals(true, row.getBoolean("b"))
    }

    @Test
    fun `缺失的可空字段按 null 处理`() {
        val schema = Schema.builder().addStringField("a").addNullableInt64Field("b").build()
        val row = RowConverters.toRow(schema, json("""{"a": "x"}"""))
        assertNull(row.getInt64("b"))
    }

    @Test
    fun `非空字段缺值时报错并指出路径`() {
        val schema = Schema.builder().addStringField("a").addInt64Field("b").build()
        val error = assertFailsWith<HDataException> { RowConverters.toRow(schema, json("""{"a": "x"}""")) }
        assertTrue("$.b" in error.message!! && "不可为空" in error.message!!)
    }

    @Test
    fun `未知字段会被拒绝而不是忽略`() {
        val schema = Schema.builder().addStringField("a").build()
        val error = assertFailsWith<HDataException> { RowConverters.toRow(schema, json("""{"a": "x", "typo": 1}""")) }
        assertTrue("typo" in error.message!!)
    }

    @Test
    fun `类型不匹配时报错`() {
        val schema = Schema.builder().addInt64Field("a").build()
        val error = assertFailsWith<HDataException> { RowConverters.toRow(schema, json("""{"a": "x"}""")) }
        assertTrue("期望是数字" in error.message!!)
    }

    @Test
    fun `DECIMAL 与 BYTES 分别按字符串和 base64 解析`() {
        val schema = Schema.builder()
            .addDecimalField("amount")
            .addByteArrayField("payload")
            .build()
        val payload = Base64.getEncoder().encodeToString("hi".toByteArray())

        val row = RowConverters.toRow(schema, json("""{"amount": "1.05", "payload": "$payload"}"""))

        assertEquals(BigDecimal("1.05"), row.getDecimal("amount"))
        assertContentEquals("hi".toByteArray(), row.getBytes("payload"))
    }

    @Test
    fun `逻辑时间类型按 ISO 文本解析`() {
        val schema = Schema.builder()
            .addLogicalTypeField("d", SqlTypes.DATE)
            .addLogicalTypeField("t", SqlTypes.TIME)
            .addLogicalTypeField("dt", SqlTypes.DATETIME)
            .addLogicalTypeField("ts", SqlTypes.TIMESTAMP)
            .build()

        val row = RowConverters.toRow(
            schema,
            json(
                """
                {"d": "2022-08-30", "t": "12:30:00", "dt": "2022-08-30T12:30:00", "ts": "2022-08-30T12:30:00Z"}
                """.trimIndent()
            ),
        )

        assertEquals(LocalDate.of(2022, 8, 30), row.getLogicalTypeValue("d", LocalDate::class.java))
        assertEquals(LocalTime.of(12, 30), row.getLogicalTypeValue("t", LocalTime::class.java))
        assertEquals(LocalDateTime.of(2022, 8, 30, 12, 30), row.getLogicalTypeValue("dt", LocalDateTime::class.java))
        assertEquals(Instant.parse("2022-08-30T12:30:00Z"), row.getLogicalTypeValue("ts", Instant::class.java))
    }

    @Test
    fun `时间文本非法时报错并带上原值`() {
        val schema = Schema.builder().addLogicalTypeField("d", SqlTypes.DATE).build()
        val error = assertFailsWith<HDataException> { RowConverters.toRow(schema, json("""{"d": "not-a-date"}""")) }
        assertTrue("not-a-date" in error.message!!)
    }

    @Test
    fun `嵌套 ROW 与数组按元素类型递归转换`() {
        val inner = Schema.builder().addInt64Field("id").build()
        val schema = Schema.builder()
            .addRowField("user", inner)
            .addArrayField("scores", Schema.FieldType.INT64)
            .build()

        val row = RowConverters.toRow(schema, json("""{"user": {"id": 9}, "scores": [1, 2, 3]}"""))

        assertEquals(9L, row.getRow("user")!!.getInt64("id"))
        assertEquals(listOf(1L, 2L, 3L), row.getArray<Long>("scores"))
    }

    @Test
    fun `数组元素出错时路径带下标`() {
        val schema = Schema.builder().addArrayField("scores", Schema.FieldType.INT64).build()
        val error = assertFailsWith<HDataException> { RowConverters.toRow(schema, json("""{"scores": [1, "x"]}""")) }
        assertTrue("$.scores[1]" in error.message!!)
    }

    @Test
    fun `推断出的 schema 能直接用于转换`() {
        val elements = elements("""{"id": 1, "name": "a"}""", """{"id": 2, "name": null}""")
        val schema = RowConverters.inferSchema(elements)

        val rows = elements.map { RowConverters.toRow(schema, it) }

        assertEquals(listOf(1L, 2L), rows.map { it.getInt64("id") })
        assertEquals(listOf("a", null), rows.map { it.getString("name") })
    }
}
