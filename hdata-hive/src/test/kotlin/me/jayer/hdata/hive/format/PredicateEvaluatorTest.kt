package me.jayer.hdata.hive.format

import java.math.BigDecimal
import java.nio.charset.StandardCharsets
import me.jayer.hdata.core.type.FieldTypes
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * `PredicateEvaluator` 的纯逻辑测试：行级过滤（[PredicateEvaluator.matches]）与
 * 列统计整段跳过（[PredicateEvaluator.canSkip]）。端到端的读取端过滤（真写真读）见
 * [me.jayer.hdata.hive.HivePipelineTest] 里以 `谓词下推` 命名的用例。
 *
 * @author wuya
 */
class PredicateEvaluatorTest {

    private val idType = FieldTypes.INT64
    private val nameType = FieldTypes.STRING

    private fun row(id: Long?, name: String?): Row {
        val schema = Schema.builder()
            .addNullableField("id", idType)
            .addNullableField("name", nameType)
            .build()
        return Row.withSchema(schema).apply {
            addValue(id)
            addValue(name)
        }.build()
    }

    private val amountType = FieldTypes.DECIMAL

    private fun amountRow(amount: BigDecimal?): Row {
        val schema = Schema.builder().addNullableField("amount", amountType).build()
        return Row.withSchema(schema).apply { addValue(amount) }.build()
    }

    @Test
    fun `matches 数值比较`() {
        val p = HivePredicate("id", PredicateOp.GT, idType, NumericValue(BigDecimal(100)))
        assertTrue(PredicateEvaluator.matches(row(150, "x"), listOf(p)))
        assertFalse(PredicateEvaluator.matches(row(50, "x"), listOf(p)))
        assertFalse(PredicateEvaluator.matches(row(100, "x"), listOf(p)))
    }

    @Test
    fun `matches 字符串比较`() {
        val p = HivePredicate("name", PredicateOp.EQ, nameType, BytesValue("b".toByteArray(StandardCharsets.UTF_8)))
        assertTrue(PredicateEvaluator.matches(row(1, "b"), listOf(p)))
        assertFalse(PredicateEvaluator.matches(row(1, "c"), listOf(p)))
    }

    @Test
    fun `matches IS NULL 与 IS NOT NULL`() {
        val isNull = HivePredicate("name", PredicateOp.IS_NULL, nameType, null)
        val isNotNull = HivePredicate("name", PredicateOp.IS_NOT_NULL, nameType, null)
        assertTrue(PredicateEvaluator.matches(row(1, null), listOf(isNull)))
        assertFalse(PredicateEvaluator.matches(row(1, "x"), listOf(isNull)))
        assertTrue(PredicateEvaluator.matches(row(1, "x"), listOf(isNotNull)))
        assertFalse(PredicateEvaluator.matches(row(1, null), listOf(isNotNull)))
    }

    @Test
    fun `matches 多个谓词是 AND`() {
        val p1 = HivePredicate("id", PredicateOp.GT, idType, NumericValue(BigDecimal(100)))
        val p2 = HivePredicate("name", PredicateOp.EQ, nameType, BytesValue("b".toByteArray(StandardCharsets.UTF_8)))
        assertTrue(PredicateEvaluator.matches(row(150, "b"), listOf(p1, p2)))
        assertFalse(PredicateEvaluator.matches(row(50, "b"), listOf(p1, p2)))
        assertFalse(PredicateEvaluator.matches(row(150, "c"), listOf(p1, p2)))
    }

    @Test
    fun `matches decimal 比较`() {
        val p = HivePredicate("amount", PredicateOp.GT, amountType, NumericValue(BigDecimal("5.50")))
        assertTrue(PredicateEvaluator.matches(amountRow(BigDecimal("6.50")), listOf(p)))
        assertFalse(PredicateEvaluator.matches(amountRow(BigDecimal("5.50")), listOf(p)))
        assertFalse(PredicateEvaluator.matches(amountRow(BigDecimal("1.00")), listOf(p)))
    }

    @Test
    fun `canSkip decimal GT 在整段最大值不够大时跳过`() {
        val p = HivePredicate("amount", PredicateOp.GT, amountType, NumericValue(BigDecimal("100.00")))
        val stats = mapOf("amount" to ColumnRangeStats(NumericValue(BigDecimal("1.00")), NumericValue(BigDecimal("50.00")), false))
        assertTrue(PredicateEvaluator.canSkip(listOf(p), stats))
        val stats2 = mapOf("amount" to ColumnRangeStats(NumericValue(BigDecimal("1.00")), NumericValue(BigDecimal("200.00")), false))
        assertFalse(PredicateEvaluator.canSkip(listOf(p), stats2))
    }

    @Test
    fun `canSkip GT 在整段最大值不够大时跳过`() {
        val p = HivePredicate("id", PredicateOp.GT, idType, NumericValue(BigDecimal(100)))
        val stats = mapOf("id" to ColumnRangeStats(NumericValue(BigDecimal(1)), NumericValue(BigDecimal(50)), false))
        assertTrue(PredicateEvaluator.canSkip(listOf(p), stats))
        val stats2 = mapOf("id" to ColumnRangeStats(NumericValue(BigDecimal(1)), NumericValue(BigDecimal(200)), false))
        assertFalse(PredicateEvaluator.canSkip(listOf(p), stats2))
    }

    @Test
    fun `canSkip EQ 在整段不含该值时跳过`() {
        val p = HivePredicate("id", PredicateOp.EQ, idType, NumericValue(BigDecimal(100)))
        val stats = mapOf("id" to ColumnRangeStats(NumericValue(BigDecimal(1)), NumericValue(BigDecimal(50)), false))
        assertTrue(PredicateEvaluator.canSkip(listOf(p), stats))
        val stats2 = mapOf("id" to ColumnRangeStats(NumericValue(BigDecimal(1)), NumericValue(BigDecimal(200)), false))
        assertFalse(PredicateEvaluator.canSkip(listOf(p), stats2))
    }

    @Test
    fun `canSkip 字符串按无符号字节序`() {
        val p = HivePredicate("name", PredicateOp.EQ, nameType, BytesValue("m".toByteArray(StandardCharsets.UTF_8)))
        // min="a" max="g"，"m" 在字节序上大于 "g"，整段不可能含 "m" → 跳过
        val stats = mapOf("name" to ColumnRangeStats(BytesValue("a".toByteArray(StandardCharsets.UTF_8)), BytesValue("g".toByteArray(StandardCharsets.UTF_8)), false))
        assertTrue(PredicateEvaluator.canSkip(listOf(p), stats))
    }

    @Test
    fun `canSkip IS NULL 在没有 null 时跳过`() {
        val p = HivePredicate("name", PredicateOp.IS_NULL, nameType, null)
        assertTrue(PredicateEvaluator.canSkip(listOf(p), mapOf("name" to ColumnRangeStats(null, null, false))))
        assertFalse(PredicateEvaluator.canSkip(listOf(p), mapOf("name" to ColumnRangeStats(null, null, true))))
    }

    @Test
    fun `canSkip IS NOT NULL 在整段全空时跳过`() {
        val p = HivePredicate("name", PredicateOp.IS_NOT_NULL, nameType, null)
        // 整列全 NULL（hasNull=true 且 allNull=true）→ 整段不可能命中 IS NOT NULL → 跳过
        assertTrue(PredicateEvaluator.canSkip(listOf(p), mapOf("name" to ColumnRangeStats(null, null, true, allNull = true))))
        // 有非 NULL 值（allNull=false）→ 不能跳过
        assertFalse(PredicateEvaluator.canSkip(listOf(p), mapOf("name" to ColumnRangeStats(null, null, true, allNull = false))))
        // 该单元根本没有统计 → 不跳过
        assertFalse(PredicateEvaluator.canSkip(listOf(p), emptyMap()))
    }

    @Test
    fun `canSkip 拿不到统计时不跳过`() {
        val p = HivePredicate("id", PredicateOp.GT, idType, NumericValue(BigDecimal(100)))
        assertFalse(PredicateEvaluator.canSkip(listOf(p), emptyMap()))
        assertFalse(PredicateEvaluator.canSkip(listOf(p), mapOf("id" to ColumnRangeStats(null, null, true))))
    }
}
