package me.jayer.hdata.jdbc.partition

import java.math.BigDecimal
import java.math.BigInteger
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.time.LocalDateTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 分区列的 long 映射：切分区靠的就是它，来回转换必须稳。
 *
 * @author wuya
 * @date 2022-08-30
 */
class PartitionConvertersTest {

    @Suppress("UNCHECKED_CAST")
    private fun <T : Any> converterOf(name: PartitionConverters) =
        name.partitionConverter as PartitionConverter<T>

    @Test
    fun `整数类型来回转换保持不变`() {
        assertEquals(7.toByte(), converterOf<Byte>(PartitionConverters.BYTE).let { it.fromLong(it.toLong(7)) })
        assertEquals(7.toShort(), converterOf<Short>(PartitionConverters.SHORT).let { it.fromLong(it.toLong(7)) })
        assertEquals(7, converterOf<Int>(PartitionConverters.INT).let { it.fromLong(it.toLong(7)) })
        assertEquals(7L, converterOf<Long>(PartitionConverters.LONG).let { it.fromLong(it.toLong(7L)) })
        assertEquals(
            BigInteger.valueOf(7),
            converterOf<BigInteger>(PartitionConverters.BIG_INTEGER).let { it.fromLong(it.toLong(BigInteger.valueOf(7))) },
        )
    }

    @Test
    fun `布尔按 0 和 1 映射`() {
        val converter = converterOf<Boolean>(PartitionConverters.BOOLEAN)

        assertEquals(1L, converter.toLong(true))
        assertEquals(0L, converter.toLong(false))
        assertEquals(true, converter.fromLong(1))
        assertEquals(false, converter.fromLong(0))
    }

    @Test
    fun `浮点与定点会截断到整数，切分区只保证单调`() {
        val double = converterOf<Double>(PartitionConverters.DOUBLE)
        assertEquals(7L, double.toLong(7.9))
        assertEquals(7.0, double.fromLong(7))

        val decimal = converterOf<BigDecimal>(PartitionConverters.BIG_DECIMAL)
        assertEquals(7L, decimal.toLong(BigDecimal("7.9")))
        assertEquals(BigDecimal.valueOf(7), decimal.fromLong(7))
    }

    @Test
    fun `DATE 按 epochDay 映射`() {
        val converter = converterOf<Date>(PartitionConverters.DATE)
        val date = Date.valueOf("2022-08-30")

        assertEquals(date.toLocalDate().toEpochDay(), converter.toLong(date))
        assertEquals(date, converter.fromLong(converter.toLong(date)))
    }

    @Test
    fun `TIME 按当天秒数映射`() {
        val converter = converterOf<Time>(PartitionConverters.TIME)
        val time = Time.valueOf("12:30:45")

        assertEquals(12 * 3600L + 30 * 60 + 45, converter.toLong(time))
        assertEquals(time, converter.fromLong(converter.toLong(time)))
    }

    @Test
    fun `TIMESTAMP 与 LOCAL_DATE_TIME 按秒映射`() {
        val timestamp = converterOf<Timestamp>(PartitionConverters.TIMESTAMP)
        val value = Timestamp.valueOf("2022-08-30 12:30:45")
        assertEquals(value, timestamp.fromLong(timestamp.toLong(value)))

        val localDateTime = converterOf<LocalDateTime>(PartitionConverters.LOCAL_DATE_TIME)
        val local = LocalDateTime.of(2022, 8, 30, 12, 30, 45)
        assertEquals(local, localDateTime.fromLong(localDateTime.toLong(local)))
    }

    @Test
    fun `映射保持单调，否则切出来的区间会错位`() {
        val converter = converterOf<Int>(PartitionConverters.INT)
        val values = listOf(-5, 0, 1, 100, 9999)

        val mapped = values.map { converter.toLong(it) }
        assertEquals(mapped.sorted(), mapped)
    }

    @Test
    fun `每个枚举项的 type 都不重复，否则按类型查找会撞车`() {
        val types = PartitionConverters.entries.map { it.type }
        assertEquals(types.size, types.distinct().size)
    }

    @Test
    fun `覆盖了常见的主键类型`() {
        val types = PartitionConverters.entries.map { it.type.javaObjectType }
        listOf(
            java.lang.Integer::class.java,
            java.lang.Long::class.java,
            BigInteger::class.java,
            BigDecimal::class.java,
            Date::class.java,
            Timestamp::class.java,
        ).forEach { assertTrue(it in types, "缺少分区类型支持: ${it.canonicalName}") }
    }
}
