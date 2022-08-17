package io.jayer.hdata.jdbc

import org.apache.beam.sdk.io.range.OffsetRange
import java.io.Serializable
import java.math.BigInteger
import java.sql.*
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlin.reflect.KClass

/**
 * @author wuya
 * @date 2022-08-17
 */
enum class PartitionHelpers(val type: KClass<out Any>, val partitionHelper: PartitionHelper) {
    INT(Int::class, object : PartitionHelper {
        override fun mapToOffsetRange(rs: ResultSet): OffsetRange? {
            return OffsetRange(rs.getLong(1), rs.getLong(2))
        }

        override fun setParameters(range: OffsetRange, ps: PreparedStatement) {
            ps.setLong(1, range.from)
            ps.setLong(2, range.to)
        }
    }),
    LONG(Long::class, object : PartitionHelper {
        override fun mapToOffsetRange(rs: ResultSet): OffsetRange? {
            return OffsetRange(rs.getLong(1), rs.getLong(2))
        }

        override fun setParameters(range: OffsetRange, ps: PreparedStatement) {
            ps.setLong(1, range.from)
            ps.setLong(2, range.to)
        }
    }),
    BIG_INTEGER(BigInteger::class, object : PartitionHelper {
        override fun mapToOffsetRange(rs: ResultSet): OffsetRange? {
            return OffsetRange(rs.getLong(1), rs.getLong(2))
        }

        override fun setParameters(range: OffsetRange, ps: PreparedStatement) {
            ps.setLong(1, range.from)
            ps.setLong(2, range.to)
        }
    }),
    DATE(Date::class, object : PartitionHelper {
        override fun mapToOffsetRange(rs: ResultSet): OffsetRange? {
            val min = rs.getDate(1)
            val max = rs.getDate(2)
            if (min == null || max == null) {
                return null
            }
            return OffsetRange(min.time, max.time)
        }

        override fun setParameters(range: OffsetRange, ps: PreparedStatement) {
            ps.setDate(1, Date(range.from))
            ps.setDate(2, Date(range.to))
        }
    }),
    TIME(Time::class, object : PartitionHelper {
        override fun mapToOffsetRange(rs: ResultSet): OffsetRange? {
            val min = rs.getTime(1)
            val max = rs.getTime(2)
            if (min == null || max == null) {
                return null
            }
            return OffsetRange(min.time, max.time)
        }

        override fun setParameters(range: OffsetRange, ps: PreparedStatement) {
            ps.setTime(1, Time(range.from))
            ps.setTime(2, Time(range.to))
        }
    }),
    TIMESTAMP(Timestamp::class, object : PartitionHelper {
        override fun mapToOffsetRange(rs: ResultSet): OffsetRange? {
            val min = rs.getTimestamp(1)
            val max = rs.getTimestamp(2)
            if (min == null || max == null) {
                return null
            }
            return OffsetRange(min.time, max.time)
        }

        override fun setParameters(range: OffsetRange, ps: PreparedStatement) {
            ps.setTimestamp(1, Timestamp(range.from))
            ps.setTimestamp(2, Timestamp(range.to))
        }
    }),
    LOCAL_DATE_TIME(LocalDateTime::class, object : PartitionHelper {
        private val defaultZoneOffset = ZoneOffset.systemDefault()

        override fun mapToOffsetRange(rs: ResultSet): OffsetRange? {
            val min = rs.getObject(1) as LocalDateTime?
            val max = rs.getObject(2) as LocalDateTime?

            if (min == null || max == null) {
                return null
            }

            return OffsetRange(
                min.atZone(defaultZoneOffset).toInstant().toEpochMilli(),
                max.atZone(defaultZoneOffset).toInstant().toEpochMilli()
            )
        }

        override fun setParameters(range: OffsetRange, ps: PreparedStatement) {
            ps.setObject(1, Instant.ofEpochMilli(range.from).atZone(defaultZoneOffset).toLocalDateTime())
            ps.setObject(2, Instant.ofEpochMilli(range.to).atZone(defaultZoneOffset).toLocalDateTime())
        }
    });

    interface PartitionHelper : Serializable {
        fun mapToOffsetRange(rs: ResultSet): OffsetRange?
        fun setParameters(range: OffsetRange, ps: PreparedStatement)
    }
}