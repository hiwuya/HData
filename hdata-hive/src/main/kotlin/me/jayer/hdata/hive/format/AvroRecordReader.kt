package me.jayer.hdata.hive.format

import me.jayer.hdata.core.type.FieldTypes
import me.jayer.hdata.hive.split.HiveFile
import org.apache.avro.Conversions
import org.apache.avro.LogicalType
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema as AvroSchema
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.SeekableInput
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.Path
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset

/**
 * Avro 容器文件的读取器。
 *
 * schema 直接取文件自带的那份，不看 metastore 上的 `avro.schema.literal` / `avro.schema.url`：
 * 文件里的 schema 才是数据真正的样子，两者不一致时以文件为准是唯一安全的选择
 * （表的 schema 被人改过而老文件没重写，这在 Avro 表上是常态）。
 *
 * 可认领的边界是**同步块**：Avro 容器文件每隔一段插一个 16 字节的同步标记，
 * `sync(position)` 定位到之后的第一个块，`pastSync(end)` 判断是否越过了区间末尾。
 *
 * @author wuya
 */
class AvroRecordReader(
    private val file: HiveFile,
    private val range: OffsetRange,
    spec: HiveReadSpec,
    partitionValues: List<Any?>,
    private val configuration: Configuration,
) : HiveRecordReader(spec, partitionValues) {

    private val fieldTypes = spec.dataFieldTypes
    private var reader: DataFileReader<GenericRecord>? = null

    override fun read(claim: OffsetClaim, output: (Row) -> Unit): Boolean {
        val path = Path(file.path)
        val fs = path.getFileSystem(configuration)
        val datumReader = GenericDatumReader<GenericRecord>().apply {
            data.addLogicalTypeConversion(Conversions.DecimalConversion())
        }
        val avroReader = DataFileReader(HadoopSeekableInput(fs.open(path), file.length), datumReader)
        reader = avroReader

        if (range.from > 0) {
            avroReader.sync(range.from)
        }
        var claimed = -1L
        while (avroReader.hasNext() && !avroReader.pastSync(range.to)) {
            // 夹到 from：同步块的起点理论上不会落在区间之前，但真落了的话
            // OffsetRangeTracker 会直接抛异常，夹一下比事后排查便宜
            val blockStart = avroReader.previousSync().coerceAtLeast(range.from)
            if (blockStart > claimed) {
                if (!claim.tryClaim(blockStart)) {
                    return false
                }
                claimed = blockStart
            }
            output(toRow(decode(avroReader.next())))
        }
        return true
    }

    private fun decode(record: GenericRecord): Array<Any?> = Array(spec.projectedDataIndexes.size) { i ->
        val index = spec.projectedDataIndexes[i]
        val name = spec.dataColumns[index].name
        val field = record.schema.fields.firstOrNull { it.name().equals(name, ignoreCase = true) }
        field?.let { toValue(record.get(it.pos()), it.schema(), fieldTypes[index]) }
    }

    private fun toValue(value: Any?, schema: AvroSchema, fieldType: Schema.FieldType): Any? {
        if (value == null) {
            return null
        }
        val resolved = unwrapUnion(schema, value)
        val target = fieldType.withNullable(false)
        resolved.logicalType?.let { logical ->
            decodeLogical(value, resolved, logical, target)?.let { return it }
        }
        return when (resolved.type) {
            AvroSchema.Type.STRING -> (value as? Utf8)?.toString() ?: value.toString()
            AvroSchema.Type.ENUM -> value.toString()
            AvroSchema.Type.BOOLEAN -> value as Boolean
            AvroSchema.Type.INT -> narrow((value as Int).toLong(), target)
            AvroSchema.Type.LONG -> narrow(value as Long, target)
            AvroSchema.Type.FLOAT -> value as Float
            AvroSchema.Type.DOUBLE -> value as Double
            AvroSchema.Type.BYTES -> toBytes(value)
            AvroSchema.Type.FIXED -> (value as GenericFixed).bytes()

            AvroSchema.Type.ARRAY -> {
                val elementType = target.collectionElementType!!
                (value as Collection<*>).map { toValue(it, resolved.elementType, elementType) }
            }

            AvroSchema.Type.MAP -> (value as Map<*, *>).entries.associate { (k, v) ->
                k.toString() to toValue(v, resolved.valueType, target.mapValueType!!)
            }

            AvroSchema.Type.RECORD -> {
                val nested = value as GenericRecord
                val rowSchema = target.rowSchema!!
                val builder = Row.withSchema(rowSchema)
                rowSchema.fields.forEach { field ->
                    val avroField = resolved.fields.firstOrNull { it.name().equals(field.name, ignoreCase = true) }
                    builder.addValue(
                        avroField?.let { toValue(nested.get(it.pos()), it.schema(), field.type) }
                    )
                }
                builder.build()
            }

            AvroSchema.Type.NULL -> null
            else -> throw UnsupportedOperationException("暂不支持的 Avro 类型: ${resolved.type}")
        }
    }

    /** union 只处理 `["null", X]` 这种可空写法，多分支 union 没有对应的 Hive 类型。 */
    private fun unwrapUnion(schema: AvroSchema, value: Any): AvroSchema {
        if (schema.type != AvroSchema.Type.UNION) {
            return schema
        }
        val branches = schema.types.filter { it.type != AvroSchema.Type.NULL }
        if (branches.size == 1) {
            return branches.single()
        }
        val index = GenericData.get().resolveUnion(schema, value)
        return schema.types[index]
    }

    private fun decodeLogical(
        value: Any,
        schema: AvroSchema,
        logical: LogicalType,
        target: Schema.FieldType,
    ): Any? = when (logical) {
        is LogicalTypes.Decimal -> when (value) {
            is BigDecimal -> value
            is ByteBuffer -> Conversions.DecimalConversion().fromBytes(value, schema, logical)
            is GenericFixed -> Conversions.DecimalConversion().fromFixed(value, schema, logical)
            else -> null
        }

        is LogicalTypes.Date -> LocalDate.ofEpochDay((value as Int).toLong())
        is LogicalTypes.TimeMillis -> LocalTime.ofNanoOfDay((value as Int).toLong() * 1_000_000L)
        is LogicalTypes.TimeMicros -> LocalTime.ofNanoOfDay((value as Long) * 1_000L)
        is LogicalTypes.TimestampMillis -> instant(Instant.ofEpochMilli(value as Long), target)
        is LogicalTypes.TimestampMicros -> instant(
            Instant.ofEpochSecond(Math.floorDiv(value as Long, 1_000_000L), Math.floorMod(value, 1_000_000L) * 1_000L),
            target,
        )

        else -> null
    }

    private fun instant(instant: Instant, target: Schema.FieldType): Any =
        if (target == FieldTypes.TIMESTAMP) instant else LocalDateTime.ofInstant(instant, ZoneOffset.UTC)

    private fun toBytes(value: Any): ByteArray = when (value) {
        is ByteBuffer -> ByteArray(value.remaining()).also { value.duplicate().get(it) }
        is ByteArray -> value
        else -> value.toString().toByteArray()
    }

    /** Avro 只有 int / long，表上声明成 tinyint / smallint 时要窄回去。 */
    private fun narrow(value: Long, target: Schema.FieldType): Any = when (target.typeName) {
        Schema.TypeName.BYTE -> value.toByte()
        Schema.TypeName.INT16 -> value.toShort()
        Schema.TypeName.INT32 -> value.toInt()
        Schema.TypeName.DECIMAL -> BigDecimal.valueOf(value)
        Schema.TypeName.DOUBLE -> value.toDouble()
        Schema.TypeName.FLOAT -> value.toFloat()
        else -> value
    }

    override fun close() {
        reader?.close()
        reader = null
    }
}

/**
 * 让 Avro 能在 Hadoop 文件系统上做随机读。
 *
 * avro 自己的 `FsInput` 在 `avro-mapred` 里，那个包会把整个 MapReduce 栈拖进来，
 * 而适配一下 [SeekableInput] 只要十几行。
 */
private class HadoopSeekableInput(
    private val input: FSDataInputStream,
    private val length: Long,
) : SeekableInput {

    override fun seek(position: Long) = input.seek(position)

    override fun tell(): Long = input.pos

    override fun length(): Long = length

    override fun read(bytes: ByteArray, offset: Int, length: Int): Int = input.read(bytes, offset, length)

    override fun close() = input.close()
}
