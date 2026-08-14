package me.jayer.hdata.hive.format

import me.jayer.hdata.hive.metastore.HiveColumn
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.common.type.HiveDecimal
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector
import org.apache.orc.CompressionKind
import org.apache.orc.OrcFile
import org.apache.orc.TypeDescription
import org.apache.orc.Writer
import org.apache.orc.impl.PhysicalFsWriter
import org.apache.orc.impl.writer.WriterEncryptionVariant
import java.math.BigDecimal
import java.nio.channels.Channels
import java.nio.channels.WritableByteChannel
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset

/**
 * 把 Beam 的 `Row` 写成 ORC 文件的 [FileIO.Sink]。
 *
 * ORC 官方的入口 `OrcFile.createWriter(path, ...)` 要一个 Hadoop `Path`，而 Beam 的
 * `FileIO.Sink` 给的是一个 `WritableByteChannel`（写的是临时文件，全部成功后才原子改名）。
 * 桥接靠 `WriterOptions.physicalWriter(...)`：塞一个自己构造的 `PhysicalFsWriter` 进去，
 * ORC 就再也不碰文件系统了，所有字节都走我们给的这个流。
 *
 * 时间戳按 UTC 写（`useUTCTimestamp(true)`），与 [OrcRecordReader] 对称。
 *
 * @author wuya
 */
class OrcSink(
    private val columns: List<HiveColumn>,
    private val schema: Schema,
    private val compression: String = "zlib",
    private val configuration: Map<String, String> = emptyMap(),
) : FileIO.Sink<Row> {

    @Transient
    private var writer: Writer? = null

    @Transient
    private var batch: org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch? = null

    @Transient
    private var typeDescription: TypeDescription? = null

    override fun open(channel: WritableByteChannel) {
        val conf = me.jayer.hdata.hive.split.HiveFileSystems.configurationOf(configuration)
        val type = TypeDescription.fromString(orcTypeString())
        val options = OrcFile.writerOptions(conf)
            .setSchema(type)
            .useUTCTimestamp(true)
            .compress(CompressionKind.valueOf(compression.uppercase()))
        // FSDataOutputStream 只是给 ORC 一个能报告当前位置的输出流，底下就是 Beam 的 channel
        val stream = FSDataOutputStream(Channels.newOutputStream(channel), null)
        options.physicalWriter(PhysicalFsWriter(stream, options, arrayOf<WriterEncryptionVariant>()))
        options.fileSystem(FileSystem.getLocal(conf))

        typeDescription = type
        writer = OrcFile.createWriter(Path("/hdata-orc-sink"), options)
        batch = type.createRowBatch()
    }

    override fun write(element: Row) {
        val rowBatch = checkNotNull(batch) { "ORC 写入器未初始化" }
        val type = checkNotNull(typeDescription)
        val rowIndex = rowBatch.size++
        schema.fields.forEachIndexed { i, field ->
            setValue(rowBatch.cols[i], rowIndex, element.getValue<Any?>(i), type.children[i], field.type)
        }
        if (rowBatch.size == rowBatch.maxSize) {
            checkNotNull(writer).addRowBatch(rowBatch)
            rowBatch.reset()
        }
    }

    override fun flush() {
        val rowBatch = checkNotNull(batch)
        if (rowBatch.size > 0) {
            checkNotNull(writer).addRowBatch(rowBatch)
            rowBatch.reset()
        }
        writer?.close()
        writer = null
    }

    /** ORC 的 `TypeDescription` 能直接解析 Hive 的类型字符串，省掉一层自己的映射。 */
    private fun orcTypeString(): String = columns.joinToString(
        separator = ",",
        prefix = "struct<",
        postfix = ">",
    ) { "${it.name}:${it.type}" }

    private fun setValue(
        vector: ColumnVector,
        rowIndex: Int,
        value: Any?,
        type: TypeDescription,
        fieldType: Schema.FieldType,
    ) {
        if (value == null) {
            vector.noNulls = false
            vector.isNull[rowIndex] = true
            return
        }
        when (type.category) {
            TypeDescription.Category.BOOLEAN ->
                (vector as LongColumnVector).vector[rowIndex] = if (value as Boolean) 1L else 0L

            TypeDescription.Category.BYTE,
            TypeDescription.Category.SHORT,
            TypeDescription.Category.INT,
            TypeDescription.Category.LONG,
            -> (vector as LongColumnVector).vector[rowIndex] = (value as Number).toLong()

            TypeDescription.Category.DATE ->
                (vector as LongColumnVector).vector[rowIndex] = (value as LocalDate).toEpochDay()

            TypeDescription.Category.FLOAT,
            TypeDescription.Category.DOUBLE,
            -> (vector as DoubleColumnVector).vector[rowIndex] = (value as Number).toDouble()

            TypeDescription.Category.STRING,
            TypeDescription.Category.VARCHAR,
            TypeDescription.Category.CHAR,
            -> (value.toString().toByteArray()).let { (vector as BytesColumnVector).setVal(rowIndex, it) }

            TypeDescription.Category.BINARY ->
                (vector as BytesColumnVector).setVal(rowIndex, value as ByteArray)

            TypeDescription.Category.DECIMAL ->
                (vector as DecimalColumnVector).set(rowIndex, HiveDecimal.create(value as BigDecimal))

            TypeDescription.Category.TIMESTAMP,
            TypeDescription.Category.TIMESTAMP_INSTANT,
            -> {
                val instant = when (value) {
                    is Instant -> value
                    is LocalDateTime -> value.toInstant(ZoneOffset.UTC)
                    else -> throw IllegalArgumentException("无法把 ${value.javaClass} 写成 ORC timestamp")
                }
                (vector as TimestampColumnVector).let {
                    it.time[rowIndex] = instant.toEpochMilli()
                    it.nanos[rowIndex] = instant.nano
                }
            }

            TypeDescription.Category.LIST -> {
                val list = vector as ListColumnVector
                val elements = (value as Iterable<*>).toList()
                list.offsets[rowIndex] = list.childCount.toLong()
                list.lengths[rowIndex] = elements.size.toLong()
                list.childCount += elements.size
                list.child.ensureSize(list.childCount, true)
                elements.forEachIndexed { i, element ->
                    setValue(
                        list.child,
                        list.offsets[rowIndex].toInt() + i,
                        element,
                        type.children[0],
                        fieldType.withNullable(false).collectionElementType!!,
                    )
                }
            }

            TypeDescription.Category.MAP -> {
                val map = vector as MapColumnVector
                val entries = (value as Map<*, *>).entries.toList()
                map.offsets[rowIndex] = map.childCount.toLong()
                map.lengths[rowIndex] = entries.size.toLong()
                map.childCount += entries.size
                map.keys.ensureSize(map.childCount, true)
                map.values.ensureSize(map.childCount, true)
                val target = fieldType.withNullable(false)
                entries.forEachIndexed { i, entry ->
                    val at = map.offsets[rowIndex].toInt() + i
                    setValue(map.keys, at, entry.key, type.children[0], target.mapKeyType!!)
                    setValue(map.values, at, entry.value, type.children[1], target.mapValueType!!)
                }
            }

            TypeDescription.Category.STRUCT -> {
                val struct = vector as StructColumnVector
                val nested = value as Row
                nested.schema.fields.forEachIndexed { i, field ->
                    if (i < struct.fields.size) {
                        setValue(struct.fields[i], rowIndex, nested.getValue<Any?>(i), type.children[i], field.type)
                    }
                }
            }

            else -> throw UnsupportedOperationException("暂不支持写入的 ORC 类型: ${type.category}")
        }
    }
}
