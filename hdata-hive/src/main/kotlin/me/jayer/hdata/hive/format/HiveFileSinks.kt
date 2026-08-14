package me.jayer.hdata.hive.format

import me.jayer.hdata.hive.format.rcfile.LazyBinaryCodec
import me.jayer.hdata.hive.format.rcfile.RcFileWriter
import me.jayer.hdata.hive.metastore.HiveColumn
import me.jayer.hdata.hive.type.HiveValues
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.Text
import java.io.OutputStream
import java.io.OutputStreamWriter
import java.nio.channels.Channels
import java.nio.channels.WritableByteChannel

/**
 * 按存储格式挑写入器，对应 Trino 的 `HiveFileWriterFactory`。
 *
 * 每种格式都实现成 Beam 的 [FileIO.Sink]，这样落盘由 `FileIO.writeDynamic()` 统一负责：
 * 各分片先写自己的临时文件，全部成功后才原子改名到位。自己在 `@Setup` 里开文件、
 * `@Teardown` 里关的写法在作业重试时会把已经写好的结果截断，`hdata-filesystem` 重构前就是这么坏的。
 *
 * @author wuya
 */
object HiveFileSinks {

    fun of(
        format: HiveStorageFormat,
        columns: List<HiveColumn>,
        schema: Schema,
        serdeParameters: Map<String, String>,
        configuration: Map<String, String>,
    ): FileIO.Sink<Row> = when (format) {
        HiveStorageFormat.TEXTFILE -> TextFileSink(LazySimpleCodec(serdeParameters))
        HiveStorageFormat.CSV -> CsvFileSink(serdeParameters)
        HiveStorageFormat.SEQUENCEFILE -> SequenceFileSink(LazySimpleCodec(serdeParameters))
        HiveStorageFormat.RCTEXT -> RcFileSink(schema, LazySimpleCodec(serdeParameters), binary = false)
        HiveStorageFormat.RCBINARY -> RcFileSink(schema, LazySimpleCodec(serdeParameters), binary = true)
        HiveStorageFormat.ORC -> OrcSink(columns, schema, configuration = configuration)
        HiveStorageFormat.PARQUET -> ParquetSink(columns, schema)
        HiveStorageFormat.AVRO -> AvroSink(columns, schema)
    }
}

/** TEXTFILE：一行一条记录，字段按 `LazySimpleSerDe` 的分隔符规则拼接。 */
class TextFileSink(private val codec: LazySimpleCodec) : FileIO.Sink<Row> {

    @Transient
    private var out: OutputStream? = null

    override fun open(channel: WritableByteChannel) {
        out = Channels.newOutputStream(channel).buffered()
    }

    override fun write(element: Row) {
        val stream = checkNotNull(out) { "文本写入器未初始化" }
        stream.write(codec.encodeRow(element).toByteArray(codec.charset))
        stream.write('\n'.code)
    }

    override fun flush() {
        out?.flush()
    }
}

/** `OpenCSVSerde` 的表。 */
class CsvFileSink(private val serdeParameters: Map<String, String>) : FileIO.Sink<Row> {

    @Transient
    private var printer: CSVPrinter? = null

    override fun open(channel: WritableByteChannel) {
        val format = CSVFormat.DEFAULT.builder()
            .setDelimiter(charParam("separatorChar", ','))
            .setQuote(charParam("quoteChar", '"'))
            .setEscape(charParam("escapeChar", '\\'))
            .build()
        printer = CSVPrinter(OutputStreamWriter(Channels.newOutputStream(channel)), format)
    }

    override fun write(element: Row) {
        // null 写成 \N 而不是空串：CSV 里空串和 null 长得一模一样，
        // 读回来 boolean 会变成 false、string 会变成 ""，一整列的 null 就这么悄悄没了
        val values = element.schema.fields.indices.map {
            element.getValue<Any?>(it)?.toString() ?: HiveValues.DEFAULT_NULL_FORMAT
        }
        checkNotNull(printer) { "CSV 写入器未初始化" }.printRecord(values)
    }

    override fun flush() {
        printer?.flush()
    }

    private fun charParam(name: String, fallback: Char): Char =
        serdeParameters[name]?.takeIf { it.isNotEmpty() }?.first() ?: fallback
}

/**
 * SEQUENCEFILE：key 写空，value 放一行文本，和 Hive 的
 * `HiveSequenceFileOutputFormat`（用的是 `HiveNullValueSequenceFileOutputFormat` 的反面）一致。
 */
class SequenceFileSink(private val codec: LazySimpleCodec) : FileIO.Sink<Row> {

    @Transient
    private var writer: SequenceFile.Writer? = null

    override fun open(channel: WritableByteChannel) {
        val configuration = org.apache.hadoop.conf.Configuration()
        // SequenceFile.Writer 要一个能报告位置的输出流，包一层就行，底下还是 Beam 的 channel
        val stream = FSDataOutputStream(Channels.newOutputStream(channel), null)
        writer = SequenceFile.createWriter(
            configuration,
            SequenceFile.Writer.stream(stream),
            SequenceFile.Writer.keyClass(NullWritable::class.java),
            SequenceFile.Writer.valueClass(Text::class.java),
        )
    }

    override fun write(element: Row) {
        checkNotNull(writer) { "SequenceFile 写入器未初始化" }
            .append(NullWritable.get(), Text(codec.encodeRow(element)))
    }

    override fun flush() {
        writer?.close()
        writer = null
    }
}

/** RCFile：列存，每个单元格按 RCTEXT / RCBINARY 各自的规则编码。 */
class RcFileSink(
    private val schema: Schema,
    private val codec: LazySimpleCodec,
    private val binary: Boolean,
) : FileIO.Sink<Row> {

    @Transient
    private var writer: RcFileWriter? = null

    override fun open(channel: WritableByteChannel) {
        writer = RcFileWriter(Channels.newOutputStream(channel), schema.fieldCount)
    }

    override fun write(element: Row) {
        val cells = Array<ByteArray?>(schema.fieldCount) { i ->
            val value = element.getValue<Any?>(i)
            val fieldType = schema.getField(i).type
            if (binary) {
                LazyBinaryCodec.encode(value, fieldType)
            } else {
                codec.encodeField(value, fieldType, level = 1).toByteArray(codec.charset)
            }
        }
        checkNotNull(writer) { "RCFile 写入器未初始化" }.append(cells)
    }

    override fun flush() {
        writer?.close()
        writer = null
    }
}
