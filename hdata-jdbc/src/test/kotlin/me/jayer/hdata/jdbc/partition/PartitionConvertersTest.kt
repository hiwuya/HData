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
import kotlin.test.assertFailsWith

/**
 * The long mapping of the partition column: splitting depends on it, so round-tripping must be solid.
 *
 * @author wuya
 * @date 2022-08-30
 */
class PartitionConvertersTest {

    @Suppress("UNCHECKED_CAST")
    private fun <T : Any> converterOf(name: PartitionConverters) =
        name.partitionConverter as PartitionConverter<T>

    @Test
    fun `integer types round-trip unchanged`() {
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
    fun `boolean maps to 0 and 1`() {
        val converter = converterOf<Boolean>(PartitionConverters.BOOLEAN)

        assertEquals(1L, converter.toLong(true))
        assertEquals(0L, converter.toLong(false))
        assertEquals(true, converter.fromLong(1))
        assertEquals(false, converter.fromLong(0))
    }

    @Test
    fun `a fixed-point decimal is explicitly rejected rather than truncated`() {
        val decimal = converterOf<BigDecimal>(PartitionConverters.BIG_DECIMAL)
        assertFailsWith<ArithmeticException> { decimal.toLong(BigDecimal("7.9")) }
        assertEquals(BigDecimal.valueOf(7), decimal.fromLong(7))
    }

    @Test
    fun `DATE maps by epochDay`() {
        val converter = converterOf<Date>(PartitionConverters.DATE)
        val date = Date.valueOf("2022-08-30")

        assertEquals(date.toLocalDate().toEpochDay(), converter.toLong(date))
        assertEquals(date, converter.fromLong(converter.toLong(date)))
    }

    @Test
    fun `TIME maps by seconds-of-day`() {
        val converter = converterOf<Time>(PartitionConverters.TIME)
        val time = Time.valueOf("12:30:45")

        assertEquals(12 * 3600L + 30 * 60 + 45, converter.toLong(time))
        assertEquals(time, converter.fromLong(converter.toLong(time)))
    }

    @Test
    fun `TIMESTAMP and LOCAL_DATE_TIME map by seconds`() {
        val timestamp = converterOf<Timestamp>(PartitionConverters.TIMESTAMP)
        val value = Timestamp.valueOf("2022-08-30 12:30:45")
        assertEquals(value, timestamp.fromLong(timestamp.toLong(value)))

        val localDateTime = converterOf<LocalDateTime>(PartitionConverters.LOCAL_DATE_TIME)
        val local = LocalDateTime.of(2022, 8, 30, 12, 30, 45)
        assertEquals(local, localDateTime.fromLong(localDateTime.toLong(local)))
    }

    @Test
    fun `a negative timestamp maps via floorDiv, avoiding a collision across the one-second boundary at zero`() {
        val converter = converterOf<Timestamp>(PartitionConverters.TIMESTAMP)
        assertEquals(-1L, converter.toLong(Timestamp(-1)))
        assertEquals(-1000L, converter.fromLong(-1).time)
    }

    @Test
    fun `a big integer beyond Long range is explicitly rejected rather than wrapping around`() {
        val converter = converterOf<BigInteger>(PartitionConverters.BIG_INTEGER)
        assertFailsWith<ArithmeticException> { converter.toLong(BigInteger.ONE.shiftLeft(80)) }
    }

    @Test
    fun `the mapping stays monotonic, otherwise split ranges would misalign`() {
        val converter = converterOf<Int>(PartitionConverters.INT)
        val values = listOf(-5, 0, 1, 100, 9999)

        val mapped = values.map { converter.toLong(it) }
        assertEquals(mapped.sorted(), mapped)
    }

    @Test
    fun `no two enum entries share a type, otherwise lookup by type would collide`() {
        val types = PartitionConverters.entries.map { it.type }
        assertEquals(types.size, types.distinct().size)
    }

    @Test
    fun `covers the common primary-key types`() {
        val types = PartitionConverters.entries.map { it.type.javaObjectType }
        listOf(
            java.lang.Integer::class.java,
            java.lang.Long::class.java,
            BigInteger::class.java,
            BigDecimal::class.java,
            Date::class.java,
            Timestamp::class.java,
        ).forEach { assertTrue(it in types, "missing partition type support: ${it.canonicalName}") }
    }
}
