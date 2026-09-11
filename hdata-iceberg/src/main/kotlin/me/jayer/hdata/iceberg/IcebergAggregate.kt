package me.jayer.hdata.iceberg

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.Combine
import org.apache.beam.sdk.transforms.DoFn
import org.apache.iceberg.FileFormat
import org.apache.iceberg.FileScanTask
import org.apache.iceberg.Table
import org.apache.iceberg.data.Record
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Types

/**
 * Iceberg push-down aggregation: COUNT is taken directly from the data files' metadata `recordCount` (reading no
 * data); MIN / MAX / SUM / AVG project only the relevant column, scan and accumulate file by file, then globally
 * merge across files — this is the approach of pushing to the read side (column-projection push-down) when Iceberg
 * does not support native aggregation push-down.
 *
 * Note: Iceberg 1.10's `InternalData.write` does not write column statistics into the manifest, so MIN/MAX cannot
 * rely on metadata and must read the projected column; SUM/AVG likewise (AVRO data files do not contain these two
 * statistics at all), but both can be accumulated per column on the read side to get correct results.
 *
 * Supported aggregation declarations (`aggregations`):
 * - `count` or `count:*`: COUNT(*)
 * - `min:<column>` / `max:<column>`: the column's min/max (result type matches the column)
 * - `sum:<column>` / `avg:<column>`: the column's sum / average (result is uniformly DOUBLE, consistent with JDBC/ES)
 */
data class AggSpec(val op: String, val column: String?) : java.io.Serializable {
    init {
        require(op in setOf("count", "min", "max", "sum", "avg")) {
            "Iceberg aggregation only supports count/min/max/sum/avg, not '$op'"
        }
        require(op == "count" || column != null) { "$op requires a column to be specified" }
    }
}

fun parseAggregations(specs: List<String>): List<AggSpec> = specs.map { raw ->
    require(raw.isNotBlank()) { "aggregations must not contain an empty declaration" }
    val (op, rawColumn) = raw.split(":", limit = 2).let {
        it[0].trim().lowercase() to it.getOrNull(1)?.trim()?.takeIf(String::isNotEmpty)
    }
    val column = when (op) {
        "count" -> {
            require(rawColumn == null || rawColumn == "*") { "count only supports count or count:*, not count:$rawColumn" }
            null
        }
        else -> rawColumn
    }
    AggSpec(op, column)
}.also { parsed ->
    // Duplicate output column names would make Beam Schema throw an obscure error, so give a clear message up front.
    val names = parsed.map { aggOutputName(it) }
    require(names.distinct().size == names.size) { "aggregations has duplicate output column names: ${names.joinToString()}" }
}

/** The field name of the aggregation result row: count / min_<column> / max_<column> / sum_<column> / avg_<column>. */
fun aggOutputName(spec: AggSpec): String =
    if (spec.op == "count") "count" else "${spec.op}_${spec.column}"

/** Schema of the aggregation result row: count → `count`(INT64); min/max same type as the column; sum/avg → DOUBLE. */
fun aggregateSchema(specs: List<AggSpec>, table: Table): Schema {
    val builder = Schema.builder()
    specs.forEach { (op, col) ->
        when (op) {
            "count" -> builder.addNullableField("count", Schema.FieldType.INT64)
            "min" -> builder.addNullableField("min_$col", typeToFieldType(findColumn(table, col).type()))
            "max" -> builder.addNullableField("max_$col", typeToFieldType(findColumn(table, col).type()))
            "sum", "avg" -> {
                val type = findColumn(table, col).type()
                require(type.typeId() in setOf(Type.TypeID.INTEGER, Type.TypeID.LONG, Type.TypeID.FLOAT, Type.TypeID.DOUBLE)) {
                    "Iceberg aggregation $op only supports numeric columns, but column [$col] has type $type"
                }
                builder.addNullableField("${op}_$col", Schema.FieldType.DOUBLE)
            }
        }
    }
    return builder.build()
}

private fun findColumn(table: Table, col: String?) =
    requireNotNull(table.schema().findField(col)) { "Aggregation column [$col] does not exist in table [${table.name()}]" }

/**
 * The partial aggregate computed for each data file, passed as the merge unit between the parallel read and the
 * global merge. `sums` / `nonNull` are aggregated by column name (SUM and AVG share the same per-column
 * accumulation; AVG is recovered via `sums/count`).
 */
data class PartialAgg(
    val count: Long = 0,
    val mins: Map<String, Comparable<*>?> = emptyMap(),
    val maxs: Map<String, Comparable<*>?> = emptyMap(),
    val sums: Map<String, Double> = emptyMap(),
    val nonNull: Map<String, Long> = emptyMap(),
) : java.io.Serializable

/**
 * The partial aggregate for a single data file.
 *
 * Without a filter: COUNT uses the file metadata's recordCount directly (reading no data); MIN/MAX/SUM/AVG project
 * only the relevant column and accumulate row by row — far cheaper than materializing whole rows into Beam Rows
 * before aggregating. Note: Iceberg 1.10's InternalData.write does not write column statistics into the manifest, so
 * MIN/MAX cannot rely on metadata either and must read the projected column.
 *
 * With a filter ([filterEvaluator] non-null): COUNT can no longer use recordCount — that is the whole file's row
 * count, including non-matching rows; it must evaluate the residual predicate row by row before counting, and
 * MIN/MAX/SUM/AVG only accumulate matching rows. In this case **all of the table's columns** are projected (the
 * predicate may reference columns outside the aggregation); the cost is reading a few extra columns, in exchange for
 * correct results.
 */
fun partialAggFromTask(
    task: FileScanTask,
    specs: List<AggSpec>,
    table: Table,
    filterEvaluator: org.apache.iceberg.expressions.Evaluator? = null,
): PartialAgg {
    val file = task.file()
    val schema = table.schema()
    val hasCount = specs.any { it.op == "count" }
    val numCols = specs.filter { it.op in setOf("min", "max", "sum", "avg") }.mapNotNull { it.column }.toSet()

    // Pure COUNT with no filter: the file metadata gives the answer directly, without reading a single byte.
    if (filterEvaluator == null && !hasCount && numCols.isEmpty()) {
        return PartialAgg()
    }

    val mins = mutableMapOf<String, Comparable<*>?>()
    val maxs = mutableMapOf<String, Comparable<*>?>()
    val colSum = mutableMapOf<String, Double>()
    val colCount = mutableMapOf<String, Long>()
    var matched = 0L

    if (filterEvaluator == null && numCols.isEmpty()) {
        matched = file.recordCount()
    } else {
        // With a filter the predicate may reference any column, so project all table columns; otherwise project only
        // the columns involved in the aggregation.
        val colsToRead = if (filterEvaluator != null) schema.columns().map { it.name() }.toSet() else numCols
        val dataColumns = schema.columns().filter { it.name() in colsToRead }
        val projSchema = org.apache.iceberg.Schema(dataColumns)
        val fileFormat = file.format()
        require(fileFormat == FileFormat.AVRO) {
            "Iceberg aggregation reads currently support only AVRO data files, but table [${table.name()}] contains a $fileFormat file: ${file.path()}"
        }
        val inputFile = table.io().newInputFile(file.path().toString())
        val records = org.apache.iceberg.InternalData.read(fileFormat, inputFile).project(projSchema).build<Record>()
        records.use {
            it.forEach { rec ->
                if (filterEvaluator != null && !filterEvaluator.eval(rec)) return@forEach
                matched++
                numCols.forEach { col -> updateColumn(rec, col, mins, maxs, colSum, colCount) }
            }
        }
    }

    val count = when {
        filterEvaluator != null -> if (hasCount) matched else 0L
        hasCount -> file.recordCount()
        else -> 0L
    }
    // Only columns actually declared with sum/avg need to carry sums/nonNull downstream, avoiding pointless Map transfer.
    val sumCols = specs.filter { it.op == "sum" || it.op == "avg" }.mapNotNull { it.column }.toSet()
    val sums = colSum.filterKeys { it in sumCols }
    val nonNull = colCount.filterKeys { it in sumCols }
    return PartialAgg(count, mins, maxs, sums, nonNull)
}

private fun updateColumn(
    rec: Record,
    col: String,
    mins: MutableMap<String, Comparable<*>?>,
    maxs: MutableMap<String, Comparable<*>?>,
    colSum: MutableMap<String, Double>,
    colCount: MutableMap<String, Long>,
) {
    val v = rec.getField(col) ?: return
    @Suppress("UNCHECKED_CAST")
    val c = (v as? Comparable<Any>) ?: return
    val curMin = mins[col]
    val curMax = maxs[col]
    if (curMin == null || (curMin as Comparable<Any>).compareTo(c) > 0) mins[col] = c
    if (curMax == null || (curMax as Comparable<Any>).compareTo(c) < 0) maxs[col] = c
    val d = (v as? Number)?.toDouble() ?: return
    colSum[col] = (colSum[col] ?: 0.0) + d
    colCount[col] = (colCount[col] ?: 0) + 1
}

/**
 * Merges each file's partial aggregate into one global [PartialAgg] (COUNT summed, MIN/MAX taken across files,
 * SUM/non-null counts accumulated). The output is still a [PartialAgg] rather than a Row directly — that way the
 * coder inside Combine is a plain SerializableCoder, avoiding Beam being unable to infer a Row coder inside Combine;
 * the actual assembly into a Row is left to the downstream [AggregateToRowFn].
 */
class AggregateCombineFn(
    private val specs: List<AggSpec>,
) : Combine.CombineFn<PartialAgg, PartialAgg, PartialAgg>(), java.io.Serializable {

    override fun createAccumulator(): PartialAgg = PartialAgg()

    override fun addInput(accumulator: PartialAgg, input: PartialAgg): PartialAgg {
        val mins = merge(accumulator.mins, input.mins, ::minOfSafe)
        val maxs = merge(accumulator.maxs, input.maxs, ::maxOfSafe)
        val sums = mergeDouble(accumulator.sums, input.sums)
        val nonNull = mergeLong(accumulator.nonNull, input.nonNull)
        return PartialAgg(accumulator.count + input.count, mins, maxs, sums, nonNull)
    }

    override fun mergeAccumulators(accumulators: MutableIterable<PartialAgg>): PartialAgg {
        var acc = createAccumulator()
        accumulators.forEach { acc = addInput(acc, it) }
        return acc
    }

    override fun extractOutput(accumulator: PartialAgg): PartialAgg = accumulator

    private fun merge(
        a: Map<String, Comparable<*>?>,
        b: Map<String, Comparable<*>?>,
        pick: (Comparable<*>, Comparable<*>) -> Comparable<*>,
    ): Map<String, Comparable<*>?> {
        val keys = a.keys + b.keys
        val out = mutableMapOf<String, Comparable<*>?>()
        for (k in keys) {
            val va = a[k]
            val vb = b[k]
            out[k] = when {
                va == null -> vb
                vb == null -> va
                else -> pick(va, vb)
            }
        }
        return out
    }

    private fun mergeDouble(a: Map<String, Double>, b: Map<String, Double>): Map<String, Double> {
        val keys = a.keys + b.keys
        return keys.associateWith { (a[it] ?: 0.0) + (b[it] ?: 0.0) }
    }

    private fun mergeLong(a: Map<String, Long>, b: Map<String, Long>): Map<String, Long> {
        val keys = a.keys + b.keys
        return keys.associateWith { (a[it] ?: 0) + (b[it] ?: 0) }
    }

    private fun minOfSafe(a: Comparable<*>, b: Comparable<*>): Comparable<*> =
        if ((a as Comparable<Any>).compareTo(b as Any) <= 0) a else b

    private fun maxOfSafe(a: Comparable<*>, b: Comparable<*>): Comparable<*> =
        if ((a as Comparable<Any>).compareTo(b as Any) >= 0) a else b
}

/**
 * Assembles the merged [PartialAgg] into the final single Row. Each column's type is already fixed in
 * [aggregateSchema], so here we just take the count/min/max/sum/avg values in specs order and fill them into the Row;
 * AVG is recovered via `sums/nonNull`, and is null when there are no non-null values.
 */
class AggregateToRowFn(
    private val specs: List<AggSpec>,
    private val outputSchema: Schema,
) : DoFn<PartialAgg, org.apache.beam.sdk.values.Row>() {

    @ProcessElement
    fun processElement(@Element element: PartialAgg, receiver: OutputReceiver<org.apache.beam.sdk.values.Row>) {
        val builder = org.apache.beam.sdk.values.Row.withSchema(outputSchema)
        specs.forEach { (op, col) ->
            when (op) {
                "count" -> builder.addValue(element.count)
                "min" -> builder.addValue(element.mins[col])
                "max" -> builder.addValue(element.maxs[col])
                "sum" -> builder.addValue(element.sums[col])
                "avg" -> {
                    val c = element.nonNull[col] ?: 0L
                    builder.addValue(if (c == 0L) null else (element.sums[col] ?: 0.0) / c)
                }
            }
        }
        receiver.output(builder.build())
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/** Iceberg primitive type → Beam FieldType (aggregation supports scalars only; nested types are out of scope). */
private fun typeToFieldType(type: org.apache.iceberg.types.Type): Schema.FieldType = when (type.typeId()) {
    Type.TypeID.LONG -> Schema.FieldType.INT64
    Type.TypeID.INTEGER -> Schema.FieldType.INT32
    Type.TypeID.STRING -> Schema.FieldType.STRING
    Type.TypeID.DOUBLE -> Schema.FieldType.DOUBLE
    Type.TypeID.FLOAT -> Schema.FieldType.FLOAT
    Type.TypeID.BOOLEAN -> Schema.FieldType.BOOLEAN
    else -> throw IllegalArgumentException("Iceberg aggregation does not support nested/non-scalar types: $type")
}
