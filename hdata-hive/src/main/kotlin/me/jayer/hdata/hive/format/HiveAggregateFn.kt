package me.jayer.hdata.hive.format

import java.util.Base64
import me.jayer.hdata.core.type.FieldTypes
import me.jayer.hdata.hive.split.HiveFile
import me.jayer.hdata.hive.split.HiveFileSystems
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector
import org.apache.orc.OrcFile
import org.apache.orc.Reader
import org.apache.orc.TypeDescription
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import java.io.Serializable
import java.math.BigDecimal
import java.math.BigInteger
import java.math.RoundingMode

/**
 * Aggregation pushdown (mirroring the Trino Hive connector's aggregation pushdown):
 * `count(*)` / `min(col)` / `max(col)` read the column statistics in the ORC/Parquet file tail without scanning a single row;
 * `sum(col)` / `avg(col)` have no column statistics available and must really scan the file once, accumulating (sum, non-null count).
 *
 * ORC uses `reader.numberOfRows` and the file-level min/max of each column from `reader.getStatistics()`;
 * Parquet uses the column statistics of each row group in `fileMetaData.blocks`, merged once across row groups.
 * Each file produces one "partial aggregate" row, and [mergeAggregatePartials] merges everything into a single result row.
 *
 * Like predicate pushdown, only numeric (byte/short/int/long/float/double/decimal) and string columns are supported;
 * other types or row-oriented formats (TEXT/CSV/SEQ/RC/Avro) have no usable column statistics, so pushdown is impossible and
 *
 * @author wuya
 */

/** Aggregate function type. */
enum class AggType {
    COUNT, MIN, MAX, SUM, AVG;

    companion object {
        fun of(name: String): AggType = entries.firstOrNull { it.name.equals(name.trim(), ignoreCase = true) }
            ?: throw IllegalArgumentException(
                "unrecognized aggregate type: $name, choose from: count / min / max / sum / avg"
            )
    }
}

/** One parsed aggregation request; the output column is named `count` / `min_<col>` / `max_<col>` / `sum_<col>` / `avg_<col>`. */
data class AggSpec(
    val type: AggType,
    /** Column name for MIN/MAX/SUM/AVG; null for COUNT. */
    val column: String?,
    /** The column's Beam type; COUNT uses INT64. */
    val fieldType: Schema.FieldType,
    /** Output field name. */
    val outputName: String,
    /** Scale kept when AVG runs over a DECIMAL column (from the column declaration); 0 for non-DECIMAL. */
    val decimalScale: Int = 0,
) : Serializable

/** Output type of AVG: DECIMAL columns stay DECIMAL, other numeric columns all produce DOUBLE (the mean may not be an integer). */
private fun avgOutputType(fieldType: Schema.FieldType): Schema.FieldType = when (fieldType.typeName) {
    Schema.TypeName.DECIMAL -> FieldTypes.DECIMAL
    else -> FieldTypes.DOUBLE
}

/** Schema of the aggregation output: one field per aggregate, INT64 for COUNT, the column's own type for MIN/MAX/SUM,
internal fun aggregateSchema(specs: List<AggSpec>): Schema {
    val builder = Schema.builder()
    specs.forEach { s ->
        when (s.type) {
            AggType.COUNT -> builder.addNullableField(s.outputName, FieldTypes.INT64)
            AggType.MIN, AggType.MAX, AggType.SUM -> builder.addNullableField(s.outputName, s.fieldType)
            AggType.AVG -> builder.addNullableField(s.outputName, avgOutputType(s.fieldType))
        }
    }
    return builder.build()
}

/**
 * Schema of a partial aggregate (one row per file): INT64 for COUNT, MIN/MAX/SUM/AVG all stored as encoded strings, so it can
 * use Beam's built-in RowCoder and stay consistent with the accumulators in [mergeAggregatePartials]. The output schema is
 * [aggregateSchema]; the two differ only in the field types of SUM/AVG.
 */
internal fun aggregateAccumSchema(specs: List<AggSpec>): Schema {
    val builder = Schema.builder()
    specs.forEach { s ->
        when (s.type) {
            AggType.COUNT -> builder.addNullableField(s.outputName, FieldTypes.INT64)
            AggType.MIN, AggType.MAX, AggType.SUM, AggType.AVG -> builder.addNullableField(s.outputName, FieldTypes.STRING)
        }
    }
    return builder.build()
}

/**
 * Each file produces one "partial aggregate" row: COUNT is that file's row count, MIN/MAX are the min/max from that file's
 * column statistics (or null). No row is scanned, only the file tail is read.
 */
class HiveAggregateFn(
    private val aggregates: List<AggSpec>,
    private val hadoopConf: Map<String, String>,
) : DoFn<HiveFile, Row>() {

    @Transient private lateinit var schema: Schema
    @Transient private lateinit var accumSchema: Schema

    @Setup
    fun setup() {
        schema = aggregateSchema(aggregates)
        accumSchema = aggregateAccumSchema(aggregates)
    }

    @ProcessElement
    fun processElement(@Element file: HiveFile, receiver: OutputReceiver<Row>) {
        val configuration = HiveFileSystems.configurationOf(hadoopConf)
        val format = HiveStorageFormat.of(file.partition.storage.storageFormat)
        // SUM / AVG have no column statistics available, so the file must really be scanned and accumulated into (sum, count);
        val sumAvgSpecs = aggregates.filter { it.type == AggType.SUM || it.type == AggType.AVG }
        val scanned = if (sumAvgSpecs.isNotEmpty()) scanSums(format, file, configuration, sumAvgSpecs) else emptyMap()
        val values = Array<Any?>(aggregates.size) { i ->
            val s = aggregates[i]
            when (s.type) {
                AggType.COUNT -> fileRowCount(format, file, configuration)
                AggType.MIN -> columnRange(format, file, s, configuration)?.first?.let { encodeRepr(it) }
                AggType.MAX -> columnRange(format, file, s, configuration)?.second?.let { encodeRepr(it) }
                AggType.SUM -> {
                    val (sum, _) = scanned[s] ?: (BigDecimal.ZERO to 0L)
                    encodeRepr(NumericValue(sum))
                }
                AggType.AVG -> {
                    val (sum, cnt) = scanned[s] ?: (BigDecimal.ZERO to 0L)
                    if (cnt == 0L) null else encodeAvg(NumericValue(sum), cnt)
                }
            }
        }
        receiver.output(Row.withSchema(accumSchema).addValues(values.toList()).build())
    }

    /** Scans the whole file, accumulating (sum, count) for each SUM/AVG column. COUNT/MIN/MAX are not computed here. */
    private fun scanSums(
        format: HiveStorageFormat,
        file: HiveFile,
        configuration: Configuration,
        specs: List<AggSpec>,
    ): Map<AggSpec, Pair<BigDecimal, Long>> {
        val result = specs.associateWith { BigDecimal.ZERO to 0L }.toMutableMap()
        when (format) {
            HiveStorageFormat.ORC -> scanOrcSums(file, configuration, specs, result)
            HiveStorageFormat.PARQUET -> scanParquetSums(file, configuration, specs, result)
            else -> throw UnsupportedOperationException("aggregation pushdown supports only ORC / Parquet, got $format")
        }
        return result
    }

    private fun scanOrcSums(
        file: HiveFile,
        configuration: Configuration,
        specs: List<AggSpec>,
        result: MutableMap<AggSpec, Pair<BigDecimal, Long>>,
    ) {
        val path = Path(file.path)
        OrcFile.createReader(
            path,
            OrcFile.readerOptions(configuration).filesystem(path.getFileSystem(configuration)).useUTCTimestamp(true),
        ).use { reader ->
            val schema = reader.schema
            val columns = specs.mapNotNull { s -> orcTopLevelColumn(schema, s.column!!)?.let { s to it } }.toMap()
            if (columns.isEmpty()) return
            // Read only the columns involved in SUM/AVG, to cut down decompression work
            val include = BooleanArray(schema.maximumId + 1)
            include[0] = true
            columns.values.forEach { ref -> for (x in ref.id..ref.type.maximumId) include[x] = true }
            val batch = schema.createRowBatch()
            val rows = reader.rows(reader.options().include(include))
            while (rows.nextBatch(batch)) {
                for ((s, ref) in columns) {
                    val col = batch.cols[ref.ordinal] ?: continue
                    val (sum, cnt) = result[s]!!
                    var newSum = sum
                    var newCnt = cnt
                    for (rowIndex in 0 until batch.size) {
                        val v = orcNumericValue(col, rowIndex, ref.type) ?: continue
                        newSum = newSum.add(v)
                        newCnt++
                    }
                    result[s] = newSum to newCnt
                }
            }
        }
    }

    private fun scanParquetSums(
        file: HiveFile,
        configuration: Configuration,
        specs: List<AggSpec>,
        result: MutableMap<AggSpec, Pair<BigDecimal, Long>>,
    ) {
        val path = Path(file.path)
        val fileSchema = ParquetFileReader.open(
            HadoopInputFile.fromPath(path, configuration),
            HadoopReadOptions.builder(configuration, path).build(),
        ).use { it.footer.fileMetaData.schema }
        val scaleByCol = specs.associate {
            it.column!! to parquetDecimalScale(fileSchema, fileSchema.fields.indexOfFirst { f -> f.name.equals(it.column, ignoreCase = true) })
        }
        ParquetReader.builder(GroupReadSupport(), path).withConf(configuration).build().use { reader ->
            var group = reader.read()
            while (group != null) {
                for (s in specs) {
                    val col = s.column!!
                    val ordinal = fileSchema.fields.indexOfFirst { f -> f.name.equals(col, ignoreCase = true) }
                    if (ordinal < 0) continue
                    if (group.getFieldRepetitionCount(ordinal) == 0) continue
                    val v = when (s.fieldType.typeName) {
                        Schema.TypeName.INT64 -> group.getLong(ordinal, 0)
                        Schema.TypeName.INT32, Schema.TypeName.INT16, Schema.TypeName.BYTE -> group.getInteger(ordinal, 0).toLong()
                        Schema.TypeName.DOUBLE -> group.getDouble(ordinal, 0)
                        Schema.TypeName.FLOAT -> group.getFloat(ordinal, 0).toDouble()
                        Schema.TypeName.DECIMAL -> {
                            val binary = group.getBinary(ordinal, 0)
                            BigDecimal(BigInteger(binary.bytes), scaleByCol[col]!!)
                        }
                        else -> null
                    } ?: continue
                    val r = NumericValue(BigDecimal(v.toString()))
                    val (sum, cnt) = result[s]!!
                    result[s] = sum.add(r.v) to cnt + 1
                }
                group = reader.read()
            }
        }
    }

    /** Reads one numeric cell out of the ORC vector (SUM/AVG only need numeric columns). Returns null for null or unsupported types. */
    private fun orcNumericValue(col: ColumnVector, rowIndex: Int, type: TypeDescription): BigDecimal? {
        val index = if (col.isRepeating) 0 else rowIndex
        if (!col.noNulls && col.isNull[index]) return null
        return when (type.category) {
            TypeDescription.Category.INT,
            TypeDescription.Category.BYTE,
            TypeDescription.Category.SHORT,
            TypeDescription.Category.LONG,
            -> BigDecimal((col as LongColumnVector).vector[index])

            TypeDescription.Category.FLOAT,
            TypeDescription.Category.DOUBLE,
            -> BigDecimal((col as DoubleColumnVector).vector[index].toDouble())

            TypeDescription.Category.DECIMAL ->
                (col as DecimalColumnVector).vector[index].hiveDecimal.bigDecimalValue()
                    .setScale(type.scale, RoundingMode.HALF_UP)

            else -> null
        }
    }

    private fun fileRowCount(format: HiveStorageFormat, file: HiveFile, configuration: Configuration): Long =
        when (format) {
            HiveStorageFormat.ORC -> {
                val path = Path(file.path)
                OrcFile.createReader(
                    path,
                    OrcFile.readerOptions(configuration).filesystem(path.getFileSystem(configuration)).useUTCTimestamp(true),
                ).use { it.numberOfRows }
            }

            HiveStorageFormat.PARQUET -> {
                val path = Path(file.path)
                ParquetFileReader.open(HadoopInputFile.fromPath(path, configuration), HadoopReadOptions.builder(configuration, path).build())
                    .use { it.rowGroups.sumOf { b -> b.rowCount } }
            }

            else -> throw UnsupportedOperationException("aggregation pushdown supports only ORC / Parquet, got $format")
        }

    /** Returns the comparable (min, max) representation of a column in this file; null when no statistics are available. */
    private fun columnRange(
        format: HiveStorageFormat,
        file: HiveFile,
        spec: AggSpec,
        configuration: Configuration,
    ): Pair<ValueRepr?, ValueRepr?>? {
        val column = spec.column ?: return null
        return when (format) {
            HiveStorageFormat.ORC -> {
                val path = Path(file.path)
                OrcFile.createReader(
                    path,
                    OrcFile.readerOptions(configuration).filesystem(path.getFileSystem(configuration)).useUTCTimestamp(true),
                ).use { reader ->
                    val ref = orcTopLevelColumn(reader.schema, column) ?: return null
                    val scale = if (ref.type.category == TypeDescription.Category.DECIMAL) ref.type.scale else 0
                    val stats = reader.getStatistics()
                    if (ref.id >= stats.size) return null
                    orcColumnRange(stats[ref.id], spec.fieldType, scale)
                }
            }

            HiveStorageFormat.PARQUET -> {
                val path = Path(file.path)
                ParquetFileReader.open(HadoopInputFile.fromPath(path, configuration), HadoopReadOptions.builder(configuration, path).build())
                    .use { reader ->
                        val fileSchema: MessageType = reader.footer.fileMetaData.schema
                        val ref = parquetTopLevelColumn(fileSchema, column) ?: return null
                        val scale = parquetDecimalScale(fileSchema, ref.ordinal)
                        var minR: ValueRepr? = null
                        var maxR: ValueRepr? = null
                        for (block in reader.rowGroups) {
                            if (ref.leafOrdinal >= block.columns.size) continue
                            val colStats = block.columns[ref.leafOrdinal].statistics
                            if (!colStats.hasNonNullValue()) continue
                            val (mn, mx) = parquetColumnRange(colStats, spec.fieldType, scale)
                            if (mn != null) minR = if (minR == null || mn.compareTo(minR) < 0) mn else minR
                            if (mx != null) maxR = if (maxR == null || mx.compareTo(maxR) > 0) mx else maxR
                        }
                        minR to maxR
                    }
            }

            else -> throw UnsupportedOperationException("aggregation pushdown supports only ORC / Parquet, got $format")
        }
    }

    private fun parquetDecimalScale(fileSchema: MessageType, ordinal: Int): Int {
        val type = fileSchema.getType(ordinal)
        return (type.logicalTypeAnnotation as? LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)?.scale ?: 0
    }
}

/**
 * Merges the per-file "partial aggregates" into the final result in one go:
 *  - COUNT is summed; MIN/MAX take the cross-file extreme (column statistics only, no row scanning);
 *  - SUM adds up the per-file sums; AVG = cross-file sum / cross-file non-null count.
 *
 * For an empty table (no files) [partials] is empty and the identity is emitted: COUNT=0, MIN/MAX/SUM=null, AVG=null.
 *
 * The accumulator itself is a schema'd [Row] whose fields correspond one-to-one to [aggregates]: INT64 for COUNT and
 * MIN/MAX/SUM/AVG stored as encoded strings (MIN/MAX/SUM use [encodeRepr], AVG uses [encodeAvg]), so it uses Beam's built-in
 * RowCoder and needs no custom Java serialization. This part of aggregation pushdown is tiny (one row per file), so it is merged
 * directly inside one DoFn after collecting all partial results through a side input, avoiding Combine's internal KV coder inference.
 */
fun mergeAggregatePartials(aggregates: List<AggSpec>, partials: List<Row>): Row {
    val schema: Schema = aggregateSchema(aggregates)
    val accumSchema: Schema = aggregateAccumSchema(aggregates)
    var acc = Row.withSchema(accumSchema)
        .addValues(
            List(aggregates.size) { i ->
                if (aggregates[i].type == AggType.COUNT) 0L else null
            },
        )
        .build()
    for (p in partials) {
        acc = Row.withSchema(accumSchema)
            .addValues(
                List(aggregates.size) { i ->
                    when (aggregates[i].type) {
                        AggType.COUNT -> (acc.getValue<Long>(i) ?: 0L) + (p.getValue<Long>(i) ?: 0L)
                        AggType.MIN -> mergeRepr(acc.getValue<String?>(i), p.getValue<String?>(i), aggregates[i].fieldType, takeMin = true)
                        AggType.MAX -> mergeRepr(acc.getValue<String?>(i), p.getValue<String?>(i), aggregates[i].fieldType, takeMin = false)
                        AggType.SUM -> mergeSum(acc.getValue<String?>(i), p.getValue<String?>(i))
                        AggType.AVG -> mergeAvg(acc.getValue<String?>(i), p.getValue<String?>(i))
                    }
                },
            )
            .build()
    }
    val values = List<Any?>(aggregates.size) { i ->
        when (aggregates[i].type) {
            AggType.COUNT -> acc.getValue<Long>(i)
            AggType.MIN, AggType.MAX, AggType.SUM -> {
                val enc = acc.getValue<String?>(i)
                if (enc == null) null else reprToValue(decodeRepr(enc), aggregates[i].fieldType)
            }
            AggType.AVG -> {
                val enc = acc.getValue<String?>(i)
                if (enc == null) null else {
                    val (sR, c) = decodeAvg(enc)
                    avgValue(sR, c, aggregates[i].decimalScale)
                }
            }
        }
    }
    return Row.withSchema(schema).addValues(values).build()
}

private fun mergeRepr(cur: String?, incoming: String?, fieldType: Schema.FieldType, takeMin: Boolean): String? {
    if (incoming == null) return cur
    if (cur == null) return incoming
    val curRepr = decodeRepr(cur)
    val incRepr = decodeRepr(incoming)
    val better = if (takeMin) incRepr.compareTo(curRepr) < 0 else incRepr.compareTo(curRepr) > 0
    return if (better) incoming else cur
}

private fun mergeSum(cur: String?, incoming: String?): String? {
    if (incoming == null) return cur
    if (cur == null) return incoming
    val curR = decodeRepr(cur) as? NumericValue ?: return incoming
    val incR = decodeRepr(incoming) as? NumericValue ?: return cur
    return encodeRepr(NumericValue(curR.v.add(incR.v)))
}

private fun mergeAvg(cur: String?, incoming: String?): String? {
    if (incoming == null) return cur
    if (cur == null) return incoming
    val (s1, c1) = decodeAvg(cur)
    val (s2, c2) = decodeAvg(incoming)
    val s1n = (s1 as? NumericValue)?.v ?: return incoming
    val s2n = (s2 as? NumericValue)?.v ?: return cur
    return encodeAvg(NumericValue(s1n.add(s2n)), c1 + c2)
}

/** Cross-file merge representation of AVG: sum (numeric repr) and the non-null row count, separated by `#` to avoid clashing
private fun encodeAvg(sumRepr: ValueRepr, count: Long): String = "A#${encodeRepr(sumRepr)}#$count"

private fun decodeAvg(s: String): Pair<ValueRepr, Long> {
    val parts = s.split("#")
    // parts = ["A", "<numEnc>", "<count>"]
    return decodeRepr(parts[1]) to parts[2].toLong()
}

private fun avgValue(sumRepr: ValueRepr, count: Long, decimalScale: Int): Any? {
    if (count == 0L) return null
    val sum = (sumRepr as NumericValue).v
    val avg = sum.divide(BigDecimal(count), 12, RoundingMode.HALF_UP)
    return if (decimalScale > 0) avg.setScale(decimalScale, RoundingMode.HALF_UP) else avg.toDouble()
}

/** Encodes a [ValueRepr] as a "type|raw value" string, used for cross-file merging of MIN/MAX/SUM (the accumulator stores strings). */
fun encodeRepr(r: ValueRepr): String = when (r) {
    is NumericValue -> "N|${r.v}"
    is BytesValue -> "B|" + Base64.getEncoder().encodeToString(r.v)
}

fun decodeRepr(s: String): ValueRepr {
    val sep = s.indexOf('|')
    val kind = s.substring(0, sep)
    val raw = s.substring(sep + 1)
    return when (kind) {
        "N" -> NumericValue(BigDecimal(raw))
        "B" -> BytesValue(Base64.getDecoder().decode(raw))
        else -> throw IllegalArgumentException("cannot decode the aggregate representative value: $s")
    }
}
