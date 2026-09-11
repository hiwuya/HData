package me.jayer.hdata.hive.format.rcfile

import me.jayer.hdata.core.type.FieldTypes
import org.apache.beam.sdk.schemas.Schema
import org.apache.hadoop.io.WritableUtils
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.io.Serializable
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset

/**
 * **Single-value** binary codec of Hive's `LazyBinarySerDe`, used for every cell of RCBINARY (`LazyBinaryColumnarSerDe`).
 *
 * The encoding rules follow Hive's `LazyBinary*` family rule by rule (the length comes from the RCFile cell length; the value
 *
 * | type | encoding |
 * |---|---|
 * | boolean / tinyint | 1 byte |
 * | smallint | 2 bytes big endian |
 * | int / bigint / date | Hadoop variable-length integer (VInt / VLong); date stores the epoch day |
 * | float / double | 4 / 8 bytes big endian, IEEE bit pattern |
 * | string / char / varchar / binary | raw bytes |
 * | decimal | VInt(scale) + VInt(byte count) + the two's complement bytes of the BigInteger |
 * | timestamp | 4 bytes (low 31 bits of seconds + flag) [+ VInt(reversed nanos)] [+ VLong(high bits of seconds)] |
 *
 * The timestamp row is the most convoluted part of the whole format: nanoseconds are stored **decimal-reversed** (123000000 is
 * stored as 321), so trailing zeros all end up in the high bits and the variable-length integer saves a few bytes.
 *
 * There is also one place where **the format itself loses information**: a zero-length cell may be either NULL or an empty
 * string, and it is always read as NULL. Telling the two apart requires switching to another format.
 *
 * **Nested types (array / map / struct) are not supported under RCBINARY** and throw outright. LazyBinary's nested encoding
 * needs extra handling of null bitmaps and element counts, and such tables are rare in practice; rather than ship an
 * implementation with no real sample to verify it against, we fail explicitly and ask the user to switch to ORC/Parquet.
 * RCTEXT (`ColumnarSerDe`) uses text encoding and supports nested types normally.
 *
 * @author wuya
 */
object LazyBinaryCodec : Serializable {

    private const val serialVersionUID: Long = 1

    /** The top bit of the seconds field is the flag "nanos or a second variable-length integer follows". */
    private const val SECONDS_FLAG = 0x80000000.toInt()

    private const val LOWEST_31_BITS = 0x7fffffff

    fun decode(bytes: ByteArray, range: IntRange, fieldType: Schema.FieldType): Any? {
        val start = range.first
        val length = range.last - range.first + 1
        if (length <= 0) {
            // A zero-length cell is always treated as null: RCBINARY cannot tell NULL from an empty string at the format level
            // — both are written as 0 bytes — and Hive's own LazyBinaryColumnarSerDe
            // reads it as null too, so we follow it.
            return null
        }
        val target = fieldType.withNullable(false)
        return when (target.typeName) {
            Schema.TypeName.BOOLEAN -> bytes[start] != 0.toByte()
            Schema.TypeName.BYTE -> bytes[start]
            Schema.TypeName.INT16 -> ((bytes[start].toInt() shl 8) or (bytes[start + 1].toInt() and 0xFF)).toShort()
            Schema.TypeName.INT32 -> RcFile.readVLong(bytes, start).value.toInt()
            Schema.TypeName.INT64 -> RcFile.readVLong(bytes, start).value
            Schema.TypeName.FLOAT -> Float.fromBits(readInt(bytes, start))
            Schema.TypeName.DOUBLE -> Double.fromBits(readLong(bytes, start))
            Schema.TypeName.STRING -> String(bytes, start, length, Charsets.UTF_8)
            Schema.TypeName.BYTES -> bytes.copyOfRange(start, start + length)
            Schema.TypeName.DECIMAL -> readDecimal(bytes, start)

            Schema.TypeName.LOGICAL_TYPE -> when (target) {
                FieldTypes.DATE -> LocalDate.ofEpochDay(RcFile.readVLong(bytes, start).value)
                FieldTypes.DATETIME -> LocalDateTime.ofInstant(readTimestamp(bytes, start), ZoneOffset.UTC)
                FieldTypes.TIMESTAMP -> readTimestamp(bytes, start)
                else -> unsupported(target)
            }

            else -> unsupported(target)
        }
    }

    fun encode(value: Any?, fieldType: Schema.FieldType): ByteArray {
        if (value == null) {
            return ByteArray(0)
        }
        val target = fieldType.withNullable(false)
        return when (target.typeName) {
            Schema.TypeName.BOOLEAN -> byteArrayOf(if (value as Boolean) 1 else 0)
            Schema.TypeName.BYTE -> byteArrayOf(value as Byte)
            Schema.TypeName.INT16 -> (value as Short).let {
                byteArrayOf((it.toInt() shr 8).toByte(), it.toByte())
            }

            Schema.TypeName.INT32 -> writeVLong((value as Int).toLong())
            Schema.TypeName.INT64 -> writeVLong(value as Long)
            Schema.TypeName.FLOAT -> intBytes((value as Float).toRawBits())
            Schema.TypeName.DOUBLE -> longBytes((value as Double).toRawBits())
            Schema.TypeName.STRING -> (value as String).toByteArray(Charsets.UTF_8)
            Schema.TypeName.BYTES -> value as ByteArray
            Schema.TypeName.DECIMAL -> writeDecimal(value as BigDecimal)

            Schema.TypeName.LOGICAL_TYPE -> when (target) {
                FieldTypes.DATE -> writeVLong((value as LocalDate).toEpochDay())
                FieldTypes.DATETIME -> writeTimestamp((value as LocalDateTime).toInstant(ZoneOffset.UTC))
                FieldTypes.TIMESTAMP -> writeTimestamp(value as Instant)
                else -> unsupported(target)
            }

            else -> unsupported(target)
        }
    }

    private fun unsupported(fieldType: Schema.FieldType): Nothing = throw UnsupportedOperationException(
        "the LazyBinary encoding of RCFile (RCBINARY) does not support $fieldType yet; please change the table to ORC / Parquet, " +
            "or use RCTEXT (ColumnarSerDe)"
    )

    private fun readInt(bytes: ByteArray, offset: Int): Int =
        ((bytes[offset].toInt() and 0xFF) shl 24) or
            ((bytes[offset + 1].toInt() and 0xFF) shl 16) or
            ((bytes[offset + 2].toInt() and 0xFF) shl 8) or
            (bytes[offset + 3].toInt() and 0xFF)

    private fun readLong(bytes: ByteArray, offset: Int): Long {
        var value = 0L
        for (i in 0 until 8) {
            value = (value shl 8) or (bytes[offset + i].toLong() and 0xFF)
        }
        return value
    }

    private fun intBytes(value: Int) = byteArrayOf(
        (value shr 24).toByte(),
        (value shr 16).toByte(),
        (value shr 8).toByte(),
        value.toByte(),
    )

    private fun longBytes(value: Long) = ByteArray(8) { i -> (value shr ((7 - i) * 8)).toByte() }

    private fun writeVLong(value: Long): ByteArray = bytes { WritableUtils.writeVLong(it, value) }

    private fun readDecimal(bytes: ByteArray, offset: Int): BigDecimal {
        val scale = RcFile.readVLong(bytes, offset)
        val byteLength = RcFile.readVLong(bytes, offset + scale.length)
        val start = offset + scale.length + byteLength.length
        val magnitude = bytes.copyOfRange(start, start + byteLength.value.toInt())
        return BigDecimal(BigInteger(magnitude), scale.value.toInt())
    }

    private fun writeDecimal(value: BigDecimal): ByteArray = bytes { out ->
        val magnitude = value.unscaledValue().toByteArray()
        WritableUtils.writeVInt(out, value.scale())
        WritableUtils.writeVInt(out, magnitude.size)
        out.write(magnitude)
    }

    /**
     * Compact encoding of seconds + nanoseconds, see the class comment.
     */
    private fun readTimestamp(bytes: ByteArray, offset: Int): Instant {
        val firstInt = readInt(bytes, offset)
        val hasDecimalOrSecondVInt = firstInt < 0
        if (!hasDecimalOrSecondVInt) {
            return Instant.ofEpochSecond((firstInt and LOWEST_31_BITS).toLong())
        }
        val nanosVLong = RcFile.readVLong(bytes, offset + 4)
        val hasSecondVInt = nanosVLong.value < 0
        val seconds = if (!hasSecondVInt) {
            (firstInt and LOWEST_31_BITS).toLong()
        } else {
            val high = RcFile.readVLong(bytes, offset + 4 + nanosVLong.length)
            (firstInt and LOWEST_31_BITS).toLong() or (high.value shl 31)
        }
        return Instant.ofEpochSecond(seconds, decodeNanos(nanosVLong.value.toInt()).toLong())
    }

    private fun writeTimestamp(instant: Instant): ByteArray = bytes { out ->
        val seconds = instant.epochSecond
        val hasSecondVInt = seconds < 0 || seconds > Int.MAX_VALUE
        val decimal = encodeNanos(instant.nano)
        var firstInt = seconds.toInt()
        firstInt = if (decimal != 0 || hasSecondVInt) firstInt or SECONDS_FLAG else firstInt and LOWEST_31_BITS
        out.write(intBytes(firstInt))
        if (hasSecondVInt || decimal != 0) {
            WritableUtils.writeVLong(out, if (hasSecondVInt) (-decimal - 1).toLong() else decimal.toLong())
        }
        if (hasSecondVInt) {
            WritableUtils.writeVLong(out, seconds shr 31)
        }
    }

    /** Nanoseconds are stored decimal-reversed, so trailing zeros land in the high bits and the variable-length integer is shorter. */
    private fun encodeNanos(nanos: Int): Int {
        if (nanos == 0) {
            return 0
        }
        var remaining = nanos
        var decimal = 0
        repeat(9) {
            decimal = decimal * 10 + remaining % 10
            remaining /= 10
        }
        return decimal
    }

    private fun decodeNanos(encoded: Int): Int {
        var value = if (encoded < 0) -encoded - 1 else encoded
        if (value == 0) {
            return 0
        }
        val digits = Math.floor(Math.log10(value.toDouble())).toInt() + 1
        var reversed = 0
        while (value != 0) {
            reversed = reversed * 10 + value % 10
            value /= 10
        }
        return if (digits < 9) reversed * Math.pow(10.0, (9 - digits).toDouble()).toInt() else reversed
    }

    private fun bytes(block: (DataOutputStream) -> Unit): ByteArray {
        val buffer = ByteArrayOutputStream(16)
        DataOutputStream(buffer).use(block)
        return buffer.toByteArray()
    }
}
