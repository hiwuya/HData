package me.jayer.hdata.hive.type

import me.jayer.hdata.core.type.FieldTypes
import org.apache.beam.sdk.schemas.Schema
import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

/**
 * Hive 类型字符串的解析。重点在**嵌套**：metastore 里的类型是一整个字符串，
 * 按逗号 split 是解不开 `map<string,array<struct<a:int,b:string>>>` 的。
 *
 * @author wuya
 */
class HiveTypesTest {

    @Test
    fun `解析基本类型`() {
        assertEquals(FieldTypes.BYTE, HiveTypes.parse("tinyint"))
        assertEquals(FieldTypes.INT16, HiveTypes.parse("smallint"))
        assertEquals(FieldTypes.INT32, HiveTypes.parse("int"))
        assertEquals(FieldTypes.INT64, HiveTypes.parse("bigint"))
        assertEquals(FieldTypes.FLOAT, HiveTypes.parse("float"))
        assertEquals(FieldTypes.DOUBLE, HiveTypes.parse("double"))
        assertEquals(FieldTypes.BOOLEAN, HiveTypes.parse("boolean"))
        assertEquals(FieldTypes.STRING, HiveTypes.parse("string"))
        assertEquals(FieldTypes.BYTES, HiveTypes.parse("binary"))
        assertEquals(FieldTypes.DATE, HiveTypes.parse("date"))
    }

    @Test
    fun `带参数的类型只取类型本身`() {
        assertEquals(FieldTypes.STRING, HiveTypes.parse("varchar(255)"))
        assertEquals(FieldTypes.STRING, HiveTypes.parse("char(3)"))
        assertEquals(FieldTypes.DECIMAL, HiveTypes.parse("decimal(10,2)"))
        assertEquals(FieldTypes.DECIMAL, HiveTypes.parse("decimal"))
        assertEquals(10 to 2, HiveTypes.decimalPrecisionAndScale("decimal(10,2)"))
        // 不带参数的 decimal 是 Hive 0.12 之前的写法，等价于 decimal(10,0)
        assertEquals(10 to 0, HiveTypes.decimalPrecisionAndScale("decimal"))
    }

    @Test
    fun `timestamp 与 timestamp with local time zone 是两种类型`() {
        // Hive 的 timestamp 不带时区，是墙上时间
        assertEquals(FieldTypes.DATETIME, HiveTypes.parse("timestamp"))
        assertEquals(FieldTypes.TIMESTAMP, HiveTypes.parse("timestamp with local time zone"))
    }

    @Test
    fun `解析嵌套类型`() {
        val type = HiveTypes.parse("map<string,array<struct<a:int,b:string>>>")
        assertEquals(Schema.TypeName.MAP, type.typeName)
        assertEquals(FieldTypes.STRING, type.mapKeyType)

        val arrayType = type.mapValueType!!
        assertEquals(Schema.TypeName.ARRAY, arrayType.typeName)

        val rowType = arrayType.collectionElementType!!
        assertEquals(Schema.TypeName.ROW, rowType.typeName)
        assertEquals(listOf("a", "b"), rowType.rowSchema!!.fieldNames)
        assertEquals(FieldTypes.INT32, rowType.rowSchema!!.getField("a").type.withNullable(false))
    }

    @Test
    fun `无法识别的类型直接抛，不做兜底`() {
        assertFailsWith<IllegalArgumentException> { HiveTypes.parse("uniontype<int,string>") }
        assertFailsWith<IllegalArgumentException> { HiveTypes.parse("blob") }
        assertFailsWith<IllegalArgumentException> { HiveTypes.parse("struct<a:int") }
        // 多余内容也要报，避免 "int garbage" 被当成 int 读进去
        assertFailsWith<IllegalArgumentException> { HiveTypes.parse("int garbage") }
    }

    @Test
    fun `Beam 类型转回 Hive 类型`() {
        assertEquals("int", HiveTypes.toHiveType(FieldTypes.INT32))
        assertEquals("timestamp", HiveTypes.toHiveType(FieldTypes.DATETIME))
        assertEquals("timestamp with local time zone", HiveTypes.toHiveType(FieldTypes.TIMESTAMP))
        assertEquals(
            "array<string>",
            HiveTypes.toHiveType(Schema.FieldType.array(FieldTypes.STRING)),
        )
    }

    @Test
    fun `分区字面量按列类型还原`() {
        assertEquals(LocalDate.of(2024, 1, 1), HiveValues.fromPartitionLiteral("2024-01-01", FieldTypes.DATE))
        assertEquals(1, HiveValues.fromPartitionLiteral("1", FieldTypes.INT32))
        // __HIVE_DEFAULT_PARTITION__ 还原成 null
        assertNull(HiveValues.fromPartitionLiteral("__HIVE_DEFAULT_PARTITION__", FieldTypes.STRING))
    }

    @Test
    fun `解析失败的值变 null 而不是抛异常`() {
        // 与 Hive 的 LazySimpleSerDe 一致：脏数据那一列变 NULL，不是整个作业挂掉
        assertNull(HiveValues.parseString("abc", FieldTypes.INT32))
        assertNull(HiveValues.parseString("2024-13-45", FieldTypes.DATE))
        assertEquals(BigDecimal("1.50"), HiveValues.parseString("1.50", FieldTypes.DECIMAL))
    }

    @Test
    fun `Hive 的 timestamp 字面量用空格分隔日期和时间`() {
        assertEquals(
            LocalDateTime.of(2024, 1, 1, 10, 30, 0),
            HiveValues.parseString("2024-01-01 10:30:00", FieldTypes.DATETIME),
        )
    }

    @Test
    fun `写入端按列名对齐，不按下标`() {
        val target = Schema.builder()
            .addNullableField("id", FieldTypes.INT64)
            .addNullableField("name", FieldTypes.STRING)
            .build()
        // 上游字段顺序与目标表相反，还多一列
        val source = Schema.builder()
            .addNullableField("name", FieldTypes.STRING)
            .addNullableField("extra", FieldTypes.STRING)
            .addNullableField("id", FieldTypes.INT32)
            .build()
        val row = org.apache.beam.sdk.values.Row.withSchema(source)
            .addValues("张三", "忽略", 7)
            .build()

        val aligned = HiveValues.align(row, target)
        assertEquals(7L, aligned.getValue<Long>("id"))
        assertEquals("张三", aligned.getValue<String>("name"))
    }

    @Test
    fun `数值窄化超出范围时直接抛，不静默截断`() {
        assertFailsWith<IllegalArgumentException> { HiveValues.coerce(300L, FieldTypes.BYTE) }
        assertEquals(100.toByte(), HiveValues.coerce(100L, FieldTypes.BYTE))
    }
}
