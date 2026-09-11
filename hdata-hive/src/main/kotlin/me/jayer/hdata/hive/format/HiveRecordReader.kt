package me.jayer.hdata.hive.format

import me.jayer.hdata.hive.metastore.HiveColumn
import me.jayer.hdata.hive.metastore.HiveTable
import me.jayer.hdata.hive.split.HiveFile
import me.jayer.hdata.hive.type.HiveTypes
import me.jayer.hdata.hive.type.HiveValues
import me.jayer.hdata.hive.SampleMethod
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.conf.Configuration
import java.io.Serializable

/**
 * Column layout of one read task: which columns to read from the file, how partition columns are filled in, and what schema to
 *
 * The output order is fixed as **projected data columns + projected partition columns**, matching Hive's `SELECT *` order (Hive
 * puts partition columns last). Use `MapToFields` for a different order; the read side does not do it.
 *
 * Both data and partition columns keep the **full table list** plus a set of projection indexes, rather than only the projected
 * columns: position-sensitive formats (TEXTFILE / CSV / RCFile / ORC with `_col0` style names) locate columns by physical index,
 * and partition values are stored in the directory name by partition column position, so dropping the original positions makes
 *
 * @author wuya
 */
data class HiveReadSpec(
    val dataColumns: List<HiveColumn>,
    val projectedDataIndexes: List<Int>,
    val partitionColumns: List<HiveColumn>,
    val projectedPartitionIndexes: List<Int>,
    /** Table properties; `skip.header.line.count` and friends live here. */
    val tableParameters: Map<String, String> = emptyMap(),
    /** Read-side predicate pushdown; empty means no filtering. */
    val predicates: List<HivePredicate> = emptyList(),
    /** Maximum number of rows to output; `<= 0` means unlimited (Trino's `LIMIT` equivalent). */
    val limit: Long = -1,
    /** Retention probability of sampling pushdown (Trino's `TABLESAMPLE` equivalent); `<= 0` or `>= 1` means no sampling. */
    val sampleFraction: Double = 1.0,
    /** Sampling pushdown method (BERNOULLI row by row / SYSTEM skip whole blocks). */
    val sampleMethod: SampleMethod = SampleMethod.BERNOULLI,
    /** Random seed for sampling pushdown; `null` means it differs on every run. */
    val sampleSeed: Long? = null,
) : Serializable {

    /** Projected data columns, in output order. */
    val projectedDataColumns: List<HiveColumn> get() = projectedDataIndexes.map { dataColumns[it] }

    val projectedPartitionColumns: List<HiveColumn> get() = projectedPartitionIndexes.map { partitionColumns[it] }

    val outputSchema: Schema get() = HiveTypes.schemaOf(projectedDataColumns + projectedPartitionColumns)

    /** Beam type of each column in the file; indexes are aligned with [dataColumns]. */
    val dataFieldTypes: List<Schema.FieldType> get() = dataColumns.map { HiveTypes.parse(it.type) }

    /** Table property `skip.header.line.count`, often used by CSV external tables. */
    val headerLineCount: Int get() = tableParameters["skip.header.line.count"]?.toIntOrNull() ?: 0

    val footerLineCount: Int get() = tableParameters["skip.footer.line.count"]?.toIntOrNull() ?: 0

    companion object {
        private const val serialVersionUID: Long = 1

        /**
         * Builds the projection from the `columns` in the config. Column names are case-insensitive (Hive always stores them
         *
         * @param columns empty means read all columns
         */
        fun of(table: HiveTable, columns: List<String> = emptyList()): HiveReadSpec {
            if (columns.isEmpty()) {
                return HiveReadSpec(
                    dataColumns = table.dataColumns,
                    projectedDataIndexes = table.dataColumns.indices.toList(),
                    partitionColumns = table.partitionColumns,
                    projectedPartitionIndexes = table.partitionColumns.indices.toList(),
                    tableParameters = table.parameters,
                )
            }
            val wanted = columns.map { it.trim().lowercase() }
            val unknown = wanted.filterNot { name -> table.columns.any { it.name.equals(name, ignoreCase = true) } }
            require(unknown.isEmpty()) {
                "these columns do not exist on table ${table.qualifiedName}: $unknown; available: ${table.columns.map { it.name }}"
            }
            return HiveReadSpec(
                dataColumns = table.dataColumns,
                projectedDataIndexes = table.dataColumns.indices.filter { table.dataColumns[it].name.lowercase() in wanted },
                partitionColumns = table.partitionColumns,
                projectedPartitionIndexes = table.partitionColumns.indices
                    .filter { table.partitionColumns[it].name.lowercase() in wanted },
                tableParameters = table.parameters,
            )
        }
    }
}

/** Claims one offset. Returns false when this piece no longer belongs to this split and the reader must stop immediately. */
fun interface OffsetClaim {
    fun tryClaim(offset: Long): Boolean
}

/**
 * Reads one Hive data file by byte range.
 *
 * Every format has a different "claimable boundary" granularity, but the protocol is the same: **claim the start offset of a batch
 * before reading that batch**. ORC uses stripes, Parquet row groups, Avro / SequenceFile / RCFile sync blocks, text lines. Only
 * then can the runtime hand the remaining work to idle workers halfway through — claiming `range.to - 1` in one go tells Beam
 * "this range is not further splittable" and is exactly what to avoid.
 */
abstract class HiveRecordReader(
    protected val spec: HiveReadSpec,
    /** Partition values already parsed by partition column type, appended after the data columns of every row as they are. */
    private val partitionValues: List<Any?>,
) : AutoCloseable {

    private val outputSchema: Schema = spec.outputSchema

    /**
     * Reads this range to the end.
     *
     * @return true when the data is exhausted; false when [OffsetClaim.tryClaim] rejected, in which case the caller **must not**
     *   claim the end of the range as well, otherwise `OffsetRangeTracker` throws immediately because "the claimed offset is
     *   less than the last attempt".
     */
    abstract fun read(claim: OffsetClaim, output: (Row) -> Unit): Boolean

    /** The length of [values] is the number of projected data columns. */
    protected fun toRow(values: Array<Any?>): Row {
        val builder = Row.withSchema(outputSchema)
        values.forEach(builder::addValue)
        partitionValues.forEach(builder::addValue)
        return builder.build()
    }

    override fun close() {}
}

/**
 * Picks a reader by storage format, mirroring Trino's `HivePageSourceProvider` picking a `HivePageSourceFactory`.
 */
object HiveRecordReaders {

    fun open(
        file: HiveFile,
        range: OffsetRange,
        spec: HiveReadSpec,
        configuration: Configuration,
    ): HiveRecordReader {
        val storage = file.partition.storage
        val format = HiveStorageFormat.of(storage.storageFormat)
        val serdeParameters = storage.serdeParameters
        val partitionValues = partitionValues(spec, file)
        return when (format) {
            HiveStorageFormat.TEXTFILE ->
                TextRecordReader(file, range, spec, partitionValues, serdeParameters, configuration)

            HiveStorageFormat.CSV ->
                CsvRecordReader(file, spec, partitionValues, serdeParameters, configuration)

            HiveStorageFormat.SEQUENCEFILE ->
                SequenceFileRecordReader(file, range, spec, partitionValues, serdeParameters, configuration)

            HiveStorageFormat.RCTEXT, HiveStorageFormat.RCBINARY ->
                RcFileRecordReader(file, range, spec, partitionValues, format, serdeParameters, configuration)

            HiveStorageFormat.ORC ->
                OrcRecordReader(file, range, spec, partitionValues, configuration)

            HiveStorageFormat.PARQUET ->
                ParquetRecordReader(file, range, spec, partitionValues, configuration)

            HiveStorageFormat.AVRO ->
                AvroRecordReader(file, range, spec, partitionValues, configuration)
        }
    }

    /**
     * Literals in the partition directory name are restored into values according to the partition column type.
     *
     * Partition values are **position dependent**: `HivePartition.values` corresponds one-to-one to the table's partition
     * columns, so the projection index can be used to fetch them directly.
     */
    fun partitionValues(spec: HiveReadSpec, file: HiveFile): List<Any?> =
        spec.projectedPartitionIndexes.map { index ->
            val literal = file.partition.values.getOrNull(index)
                ?: throw IllegalStateException(
                    "the number of values (${file.partition.values.size}) of partition [${file.partition.name}] " +
                        "does not match the number of partition columns (${spec.partitionColumns.size}) of the table"
                )
            HiveValues.fromPartitionLiteral(literal, HiveTypes.parse(spec.partitionColumns[index].type))
        }
}
