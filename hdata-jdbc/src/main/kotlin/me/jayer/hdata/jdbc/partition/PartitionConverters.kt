package me.jayer.hdata.jdbc.partition

import me.jayer.hdata.core.extension.toSqlDate
import me.jayer.hdata.core.extension.toSqlTime
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import kotlin.reflect.KClass

/**
 * @author wuya
 * @date 2022-08-17
 */
enum class PartitionConverters(val type: KClass<out Any>, val partitionConverter: me.jayer.hdata.jdbc.partition.PartitionConverter<out Any>) {
    BOOLEAN(Boolean::class, object : me.jayer.hdata.jdbc.partition.PartitionConverter<Boolean> {
        override fun toLong(value: Boolean) = if (value) 1L else 0L
        override fun fromLong(value: Long) = value > 0
    }),
    BYTE(Byte::class, object : me.jayer.hdata.jdbc.partition.PartitionConverter<Byte> {
        override fun toLong(value: Byte) = value.toLong()
        override fun fromLong(value: Long) = value.toByte()
    }),
    SHORT(Short::class, object : me.jayer.hdata.jdbc.partition.PartitionConverter<Short> {
        override fun toLong(value: Short) = value.toLong()
        override fun fromLong(value: Long) = value.toShort()
    }),
    INT(Int::class, object : me.jayer.hdata.jdbc.partition.PartitionConverter<Int> {
        override fun toLong(value: Int) = value.toLong()
        override fun fromLong(value: Long) = value.toInt()
    }),
    LONG(Long::class, object : me.jayer.hdata.jdbc.partition.PartitionConverter<Long> {
        override fun toLong(value: Long) = value
        override fun fromLong(value: Long) = value
    }),
    FLOAT(Float::class, object : me.jayer.hdata.jdbc.partition.PartitionConverter<Float> {
        override fun toLong(value: Float) = value.toLong()
        override fun fromLong(value: Long) = value.toFloat()
    }),
    DOUBLE(Double::class, object : me.jayer.hdata.jdbc.partition.PartitionConverter<Double> {
        override fun toLong(value: Double) = value.toLong()
        override fun fromLong(value: Long) = value.toDouble()
    }),
    BIG_INTEGER(BigInteger::class, object : me.jayer.hdata.jdbc.partition.PartitionConverter<BigInteger> {
        override fun toLong(value: BigInteger) = value.toLong()
        override fun fromLong(value: Long) = value.toBigInteger()
    }),
    BIG_DECIMAL(BigDecimal::class, object : me.jayer.hdata.jdbc.partition.PartitionConverter<BigDecimal> {
        override fun toLong(value: BigDecimal) = value.toLong()
        override fun fromLong(value: Long) = value.toBigDecimal()
    }),
    DATE(Date::class, object : me.jayer.hdata.jdbc.partition.PartitionConverter<Date> {
        override fun toLong(value: Date) = value.toLocalDate().toEpochDay()
        override fun fromLong(value: Long) = LocalDate.ofEpochDay(value).toSqlDate()
    }),
    TIME(Time::class, object : me.jayer.hdata.jdbc.partition.PartitionConverter<Time> {
        override fun toLong(value: Time) = value.toLocalTime().toSecondOfDay().toLong()
        override fun fromLong(value: Long) = LocalTime.ofSecondOfDay(value).toSqlTime()
    }),
    TIMESTAMP(Timestamp::class, object : me.jayer.hdata.jdbc.partition.PartitionConverter<Timestamp> {
        override fun toLong(value: Timestamp) = value.time / 1000
        override fun fromLong(value: Long) = Timestamp(value * 1000)
    }),
    LOCAL_DATE_TIME(LocalDateTime::class, object : me.jayer.hdata.jdbc.partition.PartitionConverter<LocalDateTime> {
        override fun toLong(value: LocalDateTime) = Timestamp.valueOf(value).time / 1000
        override fun fromLong(value: Long) = Timestamp(value * 1000).toLocalDateTime()
    });
}