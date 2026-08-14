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
 * Hive `LazyBinarySerDe` 的**单值**二进制编解码，RCBINARY（`LazyBinaryColumnarSerDe`）的每个单元格用它。
 *
 * 编码规则逐条对齐 Hive 的 `LazyBinary*` 系列（长度由 RCFile 的单元格长度给出，值本身不带长度）：
 *
 * | 类型 | 编码 |
 * |---|---|
 * | boolean / tinyint | 1 字节 |
 * | smallint | 2 字节大端 |
 * | int / bigint / date | Hadoop 变长整数（VInt / VLong），date 存的是 epoch day |
 * | float / double | 4 / 8 字节大端，按 IEEE 位模式 |
 * | string / char / varchar / binary | 原始字节 |
 * | decimal | VInt(标度) + VInt(字节数) + BigInteger 的二进制补码字节 |
 * | timestamp | 4 字节(秒的低 31 位 + 标志位) [+ VInt(反转的纳秒)] [+ VLong(秒的高位)] |
 *
 * timestamp 那一行是整个格式里最绕的：纳秒是**十进制反转**后再存的（123000000 存成 321），
 * 这样尾部的 0 全都落到高位，变长整数能省掉几个字节。
 *
 * 还有一处**格式本身的信息丢失**：长度为 0 的单元格既可能是 NULL 也可能是空串，
 * 读出来一律是 NULL。要区分这两者只能换别的格式。
 *
 * **嵌套类型（array / map / struct）在 RCBINARY 下不支持**，会直接抛异常。
 * LazyBinary 的嵌套编码要额外处理空值位图与元素个数，而这种表在现实中极少见；
 * 与其写一份没有真实样本验证过的实现，不如明确报错让用户换 ORC/Parquet。
 * RCTEXT（`ColumnarSerDe`）走的是文本编码，嵌套类型正常支持。
 *
 * @author wuya
 */
object LazyBinaryCodec : Serializable {

    private const val serialVersionUID: Long = 1

    /** 秒字段的最高位是"后面还有纳秒或第二个变长整数"的标志。 */
    private const val SECONDS_FLAG = 0x80000000.toInt()

    private const val LOWEST_31_BITS = 0x7fffffff

    fun decode(bytes: ByteArray, range: IntRange, fieldType: Schema.FieldType): Any? {
        val start = range.first
        val length = range.last - range.first + 1
        if (length <= 0) {
            // 长度为 0 的单元格一律当 null。
            // RCBINARY 在格式层面就分不开 NULL 和空串——两者都写成 0 字节，
            // Hive 自己的 LazyBinaryColumnarSerDe 也是按 null 读的，跟着它走。
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
        "RCFile 的 LazyBinary 编码（RCBINARY）暂不支持 $fieldType，请把表改成 ORC / Parquet，" +
            "或者用 RCTEXT（ColumnarSerDe）"
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
     * 秒 + 纳秒的紧凑编码，见类注释。
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

    /** 纳秒按十进制反转存，尾零因此落到高位，变长整数能少占几个字节。 */
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
