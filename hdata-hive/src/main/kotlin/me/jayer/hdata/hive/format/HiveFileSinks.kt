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
 * Picks a writer by storage format, mirroring Trino's `HiveFileWriterFactory`.
 *
 * Every format is implemented as a Beam [FileIO.Sink], so `FileIO.writeDynamic()` owns the writing: each shard writes its own
 * temp file and they are renamed into place atomically only after everything succeeded. Opening a file in `@Setup` and closing it
 * in `@Teardown` truncates already written results when the job retries — that is exactly how `hdata-filesystem` broke before its refactor.
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

/** TEXTFILE: one record per line, fields joined by `LazySimpleSerDe`'s delimiter rules. */
class TextFileSink(private val codec: LazySimpleCodec) : FileIO.Sink<Row> {

    @Transient
    private var out: OutputStream? = null

    override fun open(channel: WritableByteChannel) {
        out = Channels.newOutputStream(channel).buffered()
    }

    override fun write(element: Row) {
        val stream = checkNotNull(out) { "text writer is not initialized" }
        stream.write(codec.encodeRow(element).toByteArray(codec.charset))
        stream.write('\n'.code)
    }

    override fun flush() {
        out?.flush()
    }
}

/** Tables using `OpenCSVSerde`. */
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
        // null is written as \\N rather than an empty string: in CSV an empty string and null look exactly the same, and reading
        // it back would turn boolean into false and string into "", silently wiping out a whole column of nulls
        val values = element.schema.fields.indices.map {
            element.getValue<Any?>(it)?.toString() ?: HiveValues.DEFAULT_NULL_FORMAT
        }
        checkNotNull(printer) { "CSV writer is not initialized" }.printRecord(values)
    }

    override fun flush() {
        printer?.flush()
    }

    private fun charParam(name: String, fallback: Char): Char =
        serdeParameters[name]?.takeIf { it.isNotEmpty() }?.first() ?: fallback
}

/**
 * SEQUENCEFILE: an empty key with one line of text as the value, consistent with Hive's
 * `HiveSequenceFileOutputFormat` (the inverse of `HiveNullValueSequenceFileOutputFormat`).
 */
class SequenceFileSink(private val codec: LazySimpleCodec) : FileIO.Sink<Row> {

    @Transient
    private var writer: SequenceFile.Writer? = null

    override fun open(channel: WritableByteChannel) {
        val configuration = org.apache.hadoop.conf.Configuration()
        // SequenceFile.Writer wants an output stream that can report its position, so wrap one; underneath it is still Beam's channel
        val stream = FSDataOutputStream(Channels.newOutputStream(channel), null)
        writer = SequenceFile.createWriter(
            configuration,
            SequenceFile.Writer.stream(stream),
            SequenceFile.Writer.keyClass(NullWritable::class.java),
            SequenceFile.Writer.valueClass(Text::class.java),
        )
    }

    override fun write(element: Row) {
        checkNotNull(writer) { "SequenceFile writer is not initialized" }
            .append(NullWritable.get(), Text(codec.encodeRow(element)))
    }

    override fun flush() {
        writer?.close()
        writer = null
    }
}

/** RCFile: columnar, each cell encoded by the RCTEXT / RCBINARY rules respectively. */
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
        checkNotNull(writer) { "RCFile writer is not initialized" }.append(cells)
    }

    override fun flush() {
        writer?.close()
        writer = null
    }
}
