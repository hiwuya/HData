package me.jayer.hdata.hive.format

import me.jayer.hdata.core.type.FieldTypes
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.example.GroupWriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.OutputFile
import org.apache.parquet.io.PositionOutputStream
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types
import java.io.OutputStream
import java.math.BigDecimal
import java.nio.channels.Channels
import java.nio.channels.WritableByteChannel
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset

/**
 * A [FileIO.Sink] that writes Beam `Row`s as a Parquet file.
 *
 * parquet wants an `OutputFile` + `PositionOutputStream` (it needs the current offset when writing the file tail), while Beam
 * hands over a `WritableByteChannel`; a small adapter that counts bytes bridges the two —
 * Beam's own `ParquetIO` does the same.
 *
 * Timestamps are always written as `int64 + TIMESTAMP(MICROS)`, not as early Hive's int96: int96 has long been deprecated in
 * parquet and is only kept compatible on the read side.
 *
 * @author wuya
 */
class ParquetSink(
    private val columns: List<me.jayer.hdata.hive.metastore.HiveColumn>,
    private val schema: Schema,
    private val compression: String = "snappy",
) : FileIO.Sink<Row> {

    /**
     * Precision and scale of top-level decimal columns, taken from `decimal(10,2)` in the table definition.
     *
     * Beam's `DECIMAL` carries no precision or scale, so writing everything as `decimal(38,18)` would turn `1.50` into
     * `1.500000000000000000` when read back — numerically equal but not `equals`, which breaks any downstream comparison.
     * Decimals inside nested types cannot see the table definition and fall back to (38,18).
     */
    private val decimalTypes: Map<String, Pair<Int, Int>> = columns
        .filter { it.type.trim().lowercase().startsWith("decimal") || it.type.trim().lowercase().startsWith("numeric") }
        .associate { it.name.lowercase() to me.jayer.hdata.hive.type.HiveTypes.decimalPrecisionAndScale(it.type) }

    @Transient
    private var writer: ParquetWriter<Group>? = null

    @Transient
    private var messageType: MessageType? = null

    override fun open(channel: WritableByteChannel) {
        val type = toMessageType()
        messageType = type
        val configuration = Configuration()
        GroupWriteSupport.setSchema(type, configuration)
        writer = ExampleParquetWriter.builder(ChannelOutputFile(channel))
            .withConf(configuration)
            .withType(type)
            .withCompressionCodec(CompressionCodecName.fromConf(compression.uppercase()))
            .build()
    }

    override fun write(element: Row) {
        val type = checkNotNull(messageType) { "Parquet writer is not initialized" }
        val group = SimpleGroup(type)
        schema.fields.forEachIndexed { i, field ->
            addValue(group, type.getType(i), i, element.getValue<Any?>(i), field.type)
        }
        checkNotNull(writer).write(group)
    }

    override fun flush() {
        writer?.close()
        writer = null
    }

    private fun addValue(group: Group, type: Type, index: Int, value: Any?, fieldType: Schema.FieldType) {
        if (value == null) {
            // parquet's optional fields are null when simply not written, no explicit marker needed
            return
        }
        val target = fieldType.withNullable(false)
        if (!type.isPrimitive) {
            val nested = group.addGroup(index)
            when (type.logicalTypeAnnotation) {
                is LogicalTypeAnnotation.ListLogicalTypeAnnotation -> {
                    val elementType = type.asGroupType().getType(0).asGroupType()
                    (value as Iterable<*>).forEach { element ->
                        val entry = nested.addGroup(0)
                        addValue(entry, elementType.getType(0), 0, element, target.collectionElementType!!)
                    }
                }

                is LogicalTypeAnnotation.MapLogicalTypeAnnotation -> {
                    val keyValueType = type.asGroupType().getType(0).asGroupType()
                    (value as Map<*, *>).forEach { (k, v) ->
                        val entry = nested.addGroup(0)
                        addValue(entry, keyValueType.getType(0), 0, k, target.mapKeyType!!)
                        addValue(entry, keyValueType.getType(1), 1, v, target.mapValueType!!)
                    }
                }

                else -> {
                    val row = value as Row
                    val groupType = type.asGroupType()
                    row.schema.fields.forEachIndexed { i, field ->
                        addValue(nested, groupType.getType(i), i, row.getValue<Any?>(i), field.type)
                    }
                }
            }
            return
        }
        when (target.typeName) {
            Schema.TypeName.BOOLEAN -> group.add(index, value as Boolean)
            Schema.TypeName.BYTE, Schema.TypeName.INT16, Schema.TypeName.INT32 ->
                group.add(index, (value as Number).toInt())

            Schema.TypeName.INT64 -> group.add(index, (value as Number).toLong())
            Schema.TypeName.FLOAT -> group.add(index, (value as Number).toFloat())
            Schema.TypeName.DOUBLE -> group.add(index, (value as Number).toDouble())
            Schema.TypeName.STRING -> group.add(index, value.toString())
            Schema.TypeName.BYTES -> group.add(index, Binary.fromConstantByteArray(value as ByteArray))
            Schema.TypeName.DECIMAL -> {
                val scale = decimalTypes[schema.getField(index).name.lowercase()]?.second ?: DEFAULT_SCALE
                val scaled = (value as BigDecimal).setScale(scale, java.math.RoundingMode.HALF_UP)
                group.add(index, Binary.fromConstantByteArray(scaled.unscaledValue().toByteArray()))
            }

            Schema.TypeName.LOGICAL_TYPE -> when (target) {
                FieldTypes.DATE -> group.add(index, (value as LocalDate).toEpochDay().toInt())
                FieldTypes.DATETIME -> group.add(index, micros((value as LocalDateTime).toInstant(ZoneOffset.UTC)))
                FieldTypes.TIMESTAMP -> group.add(index, micros(value as Instant))
                FieldTypes.TIME -> group.add(index, value.toString())
                else -> throw UnsupportedOperationException("unsupported Parquet logical type to write: $target")
            }

            else -> throw UnsupportedOperationException("unsupported Parquet type to write: $target")
        }
    }

    private fun micros(instant: Instant): Long = instant.epochSecond * 1_000_000L + instant.nano / 1_000L

    /** Beam schema -> parquet schema. Columns are always optional; Hive columns are all nullable. */
    private fun toMessageType(): MessageType {
        val builder = Types.buildMessage()
        schema.fields.forEach { field ->
            builder.addField(toType(field.name, field.type))
        }
        return builder.named("hive_schema")
    }

    private fun toType(name: String, fieldType: Schema.FieldType): Type {
            val target = fieldType.withNullable(false)
            return when (target.typeName) {
                Schema.TypeName.BOOLEAN -> primitive(name, PrimitiveType.PrimitiveTypeName.BOOLEAN)
                Schema.TypeName.BYTE, Schema.TypeName.INT16, Schema.TypeName.INT32 ->
                    primitive(name, PrimitiveType.PrimitiveTypeName.INT32)

                Schema.TypeName.INT64 -> primitive(name, PrimitiveType.PrimitiveTypeName.INT64)
                Schema.TypeName.FLOAT -> primitive(name, PrimitiveType.PrimitiveTypeName.FLOAT)
                Schema.TypeName.DOUBLE -> primitive(name, PrimitiveType.PrimitiveTypeName.DOUBLE)
                Schema.TypeName.BYTES -> primitive(name, PrimitiveType.PrimitiveTypeName.BINARY)

                Schema.TypeName.STRING -> Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                    .`as`(LogicalTypeAnnotation.stringType())
                    .named(name)

                // Precision and scale come from decimal(p,s) in the table definition; nested types cannot see it, so fall back to (38,18)
                Schema.TypeName.DECIMAL -> decimalTypes[name.lowercase()].let { declared ->
                    Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .`as`(LogicalTypeAnnotation.decimalType(declared?.second ?: DEFAULT_SCALE, declared?.first ?: DEFAULT_PRECISION))
                        .named(name)
                }

                Schema.TypeName.ARRAY, Schema.TypeName.ITERABLE -> Types.optionalList()
                    .element(toType("element", target.collectionElementType!!))
                    .named(name)

                Schema.TypeName.MAP -> Types.optionalMap()
                    .key(toType("key", target.mapKeyType!!.withNullable(false)))
                    .value(toType("value", target.mapValueType!!))
                    .named(name)

                Schema.TypeName.ROW -> Types.optionalGroup().apply {
                    target.rowSchema!!.fields.forEach { addField(toType(it.name, it.type)) }
                }.named(name)

                Schema.TypeName.LOGICAL_TYPE -> when (target) {
                    FieldTypes.DATE -> Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .`as`(LogicalTypeAnnotation.dateType())
                        .named(name)

                    FieldTypes.DATETIME -> Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .`as`(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
                        .named(name)

                    FieldTypes.TIMESTAMP -> Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .`as`(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                        .named(name)

                    FieldTypes.TIME -> Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .`as`(LogicalTypeAnnotation.stringType())
                        .named(name)

                    else -> throw UnsupportedOperationException("unsupported Parquet logical type to write: $target")
                }

                else -> throw UnsupportedOperationException("unsupported Parquet type to write: $target")
            }
        }

    private fun primitive(name: String, type: PrimitiveType.PrimitiveTypeName): Type =
        Types.optional(type).named(name)

    private companion object {
        /** Decimal inside a nested type cannot see the table definition, so declare Hive's upper bound. */
        const val DEFAULT_PRECISION = 38
        const val DEFAULT_SCALE = 18
    }
}

/**
 * Wraps Beam's `WritableByteChannel` into the `OutputFile` parquet wants.
 *
 * parquet records the offset of every block when writing the file tail, so the stream must be able to report "how many bytes
 * have been written" — the only extra thing this class does.
 */
private class ChannelOutputFile(private val channel: WritableByteChannel) : OutputFile {

    override fun create(blockSizeHint: Long): PositionOutputStream = CountingPositionOutputStream(
        Channels.newOutputStream(channel)
    )

    override fun createOrOverwrite(blockSizeHint: Long): PositionOutputStream = create(blockSizeHint)

    override fun supportsBlockSize(): Boolean = false

    override fun defaultBlockSize(): Long = 0

    private class CountingPositionOutputStream(private val delegate: OutputStream) : PositionOutputStream() {

        private var position: Long = 0

        override fun getPos(): Long = position

        override fun write(b: Int) {
            delegate.write(b)
            position++
        }

        override fun write(b: ByteArray, off: Int, len: Int) {
            delegate.write(b, off, len)
            position += len
        }

        override fun flush() = delegate.flush()

        /**
         * **Does not close the underlying channel**: the channel's lifecycle belongs to Beam's `FileIO`, which still renames the
         * temp file after writing. Closing it here would leave the rename with a closed handle.
         */
        override fun close() {
            delegate.flush()
        }
    }
}
