package me.jayer.hdata.hive.format

import me.jayer.hdata.core.type.FieldTypes
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema as AvroSchema
import org.apache.avro.SchemaBuilder
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.WritableByteChannel
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset

/**
 * 把 Beam 的 `Row` 写成 Avro 容器文件的 [FileIO.Sink]。
 *
 * Avro 的 `DataFileWriter` 本来就接受普通的 `OutputStream`，是八种格式里唯一不需要适配的。
 *
 * decimal 写成 `bytes` + decimal 逻辑类型（Hive 的 AvroSerDe 也是这么存的）；
 * timestamp 写成 `long` + `timestamp-millis`。
 *
 * @author wuya
 */
class AvroSink(
    private val columns: List<me.jayer.hdata.hive.metastore.HiveColumn>,
    private val schema: Schema,
    private val recordName: String = "hdata_record",
    private val compression: String = "deflate",
) : FileIO.Sink<Row> {

    /**
     * 顶层 decimal 列的精度与标度，取自表定义里的 `decimal(10,2)`。
     * 统一按 `decimal(38,18)` 写的话，`1.50` 读回来会变成 `1.500000000000000000`，
     * 数值相等但 `equals` 不成立。嵌套类型取不到表定义，退回 (38,18)。
     */
    private val decimalTypes: Map<String, Pair<Int, Int>> = columns
        .filter { it.type.trim().lowercase().startsWith("decimal") || it.type.trim().lowercase().startsWith("numeric") }
        .associate { it.name.lowercase() to me.jayer.hdata.hive.type.HiveTypes.decimalPrecisionAndScale(it.type) }

    @Transient
    private var writer: DataFileWriter<GenericRecord>? = null

    @Transient
    private var avroSchema: AvroSchema? = null

    override fun open(channel: WritableByteChannel) {
        val target = toAvroSchema(schema, recordName, decimalTypes)
        avroSchema = target
        writer = DataFileWriter(GenericDatumWriter<GenericRecord>(target))
            .setCodec(codec())
            .create(target, Channels.newOutputStream(channel))
    }

    override fun write(element: Row) {
        val target = checkNotNull(avroSchema) { "Avro 写入器未初始化" }
        checkNotNull(writer).append(toRecord(element, target))
    }

    override fun flush() {
        writer?.close()
        writer = null
    }

    private fun codec(): CodecFactory = when (compression.lowercase()) {
        "null", "none", "uncompressed" -> CodecFactory.nullCodec()
        "snappy" -> CodecFactory.snappyCodec()
        "bzip2" -> CodecFactory.bzip2Codec()
        "zstandard", "zstd" -> CodecFactory.zstandardCodec(CodecFactory.DEFAULT_ZSTANDARD_LEVEL)
        else -> CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL)
    }

    private fun toRecord(row: Row, target: AvroSchema): GenericRecord {
        val record = GenericData.Record(target)
        row.schema.fields.forEachIndexed { i, field ->
            record.put(i, toAvroValue(row.getValue<Any?>(i), field.type, target.fields[i].schema(), field.name))
        }
        return record
    }

    private fun toAvroValue(
        value: Any?,
        fieldType: Schema.FieldType,
        target: AvroSchema,
        fieldName: String = "",
    ): Any? {
        if (value == null) {
            return null
        }
        val resolved = if (target.type == AvroSchema.Type.UNION) {
            target.types.first { it.type != AvroSchema.Type.NULL }
        } else {
            target
        }
        val source = fieldType.withNullable(false)
        return when (source.typeName) {
            Schema.TypeName.BOOLEAN, Schema.TypeName.FLOAT, Schema.TypeName.DOUBLE, Schema.TypeName.INT64 -> value
            Schema.TypeName.BYTE, Schema.TypeName.INT16, Schema.TypeName.INT32 -> (value as Number).toInt()
            Schema.TypeName.STRING -> value.toString()
            Schema.TypeName.BYTES -> ByteBuffer.wrap(value as ByteArray)
            Schema.TypeName.DECIMAL -> {
                val scale = decimalTypes[fieldName.lowercase()]?.second ?: DEFAULT_SCALE
                ByteBuffer.wrap(
                    (value as BigDecimal).setScale(scale, java.math.RoundingMode.HALF_UP).unscaledValue().toByteArray()
                )
            }

            Schema.TypeName.ARRAY, Schema.TypeName.ITERABLE -> (value as Iterable<*>).map {
                toAvroValue(it, source.collectionElementType!!, resolved.elementType)
            }

            Schema.TypeName.MAP -> (value as Map<*, *>).entries.associate { (k, v) ->
                k.toString() to toAvroValue(v, source.mapValueType!!, resolved.valueType)
            }

            Schema.TypeName.ROW -> toRecord(value as Row, resolved)

            Schema.TypeName.LOGICAL_TYPE -> when (source) {
                FieldTypes.DATE -> (value as LocalDate).toEpochDay().toInt()
                FieldTypes.DATETIME -> (value as LocalDateTime).toInstant(ZoneOffset.UTC).toEpochMilli()
                FieldTypes.TIMESTAMP -> (value as Instant).toEpochMilli()
                FieldTypes.TIME -> ((value as LocalTime).toNanoOfDay() / 1_000_000L).toInt()
                else -> throw UnsupportedOperationException("暂不支持写入 Avro 的逻辑类型: $source")
            }

            else -> throw UnsupportedOperationException("暂不支持写入 Avro 的类型: $source")
        }
    }

    companion object {

        /** 嵌套类型里的 decimal 取不到表定义，按 Hive 的上限声明，与 [ParquetSink] 一致。 */
        private const val DEFAULT_PRECISION = 38
        private const val DEFAULT_SCALE = 18

        /** Beam schema -> Avro schema。每个字段都写成 `["null", X]` 的可空联合。 */
        fun toAvroSchema(
            schema: Schema,
            recordName: String,
            decimalTypes: Map<String, Pair<Int, Int>> = emptyMap(),
        ): AvroSchema {
            val fields = SchemaBuilder.record(recordName).namespace("me.jayer.hdata").fields()
            schema.fields.forEach { field ->
                val type = toAvroType(field.type, "${recordName}_${field.name}", decimalTypes[field.name.lowercase()])
                fields.name(field.name).type(nullable(type)).withDefault(null)
            }
            return fields.endRecord()
        }

        private fun nullable(type: AvroSchema): AvroSchema =
            AvroSchema.createUnion(AvroSchema.create(AvroSchema.Type.NULL), type)

        private fun toAvroType(
            fieldType: Schema.FieldType,
            name: String,
            decimalType: Pair<Int, Int>? = null,
        ): AvroSchema {
            val target = fieldType.withNullable(false)
            return when (target.typeName) {
                Schema.TypeName.BOOLEAN -> AvroSchema.create(AvroSchema.Type.BOOLEAN)
                Schema.TypeName.BYTE, Schema.TypeName.INT16, Schema.TypeName.INT32 ->
                    AvroSchema.create(AvroSchema.Type.INT)

                Schema.TypeName.INT64 -> AvroSchema.create(AvroSchema.Type.LONG)
                Schema.TypeName.FLOAT -> AvroSchema.create(AvroSchema.Type.FLOAT)
                Schema.TypeName.DOUBLE -> AvroSchema.create(AvroSchema.Type.DOUBLE)
                Schema.TypeName.STRING -> AvroSchema.create(AvroSchema.Type.STRING)
                Schema.TypeName.BYTES -> AvroSchema.create(AvroSchema.Type.BYTES)

                Schema.TypeName.DECIMAL ->
                    LogicalTypes.decimal(decimalType?.first ?: DEFAULT_PRECISION, decimalType?.second ?: DEFAULT_SCALE)
                        .addToSchema(AvroSchema.create(AvroSchema.Type.BYTES))

                Schema.TypeName.ARRAY, Schema.TypeName.ITERABLE ->
                    AvroSchema.createArray(nullable(toAvroType(target.collectionElementType!!, "${name}_element")))

                Schema.TypeName.MAP ->
                    AvroSchema.createMap(nullable(toAvroType(target.mapValueType!!, "${name}_value")))

                Schema.TypeName.ROW -> toAvroSchema(target.rowSchema!!, name)

                Schema.TypeName.LOGICAL_TYPE -> when (target) {
                    FieldTypes.DATE -> LogicalTypes.date().addToSchema(AvroSchema.create(AvroSchema.Type.INT))
                    FieldTypes.DATETIME, FieldTypes.TIMESTAMP ->
                        LogicalTypes.timestampMillis().addToSchema(AvroSchema.create(AvroSchema.Type.LONG))

                    FieldTypes.TIME -> LogicalTypes.timeMillis().addToSchema(AvroSchema.create(AvroSchema.Type.INT))
                    else -> throw UnsupportedOperationException("暂不支持写入 Avro 的逻辑类型: $target")
                }

                else -> throw UnsupportedOperationException("暂不支持写入 Avro 的类型: $target")
            }
        }
    }
}
