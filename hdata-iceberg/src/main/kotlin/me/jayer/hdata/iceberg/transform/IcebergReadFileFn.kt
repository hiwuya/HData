package me.jayer.hdata.iceberg.transform

import me.jayer.hdata.iceberg.IcebergReadConfig
import me.jayer.hdata.iceberg.internal.IcebergCatalogs
import me.jayer.hdata.iceberg.internal.recordToRow
import me.jayer.hdata.iceberg.internal.validateReadableSchema
import me.jayer.hdata.iceberg.parseIcebergFilter
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.apache.iceberg.FileFormat
import org.apache.iceberg.Table
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.Record
import org.apache.iceberg.expressions.Evaluator
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.io.CloseableIterable

/**
 * Reads a single Iceberg data file and maps each row to a Row ([IcebergFileSplit] is the basic unit of parallelism).
 *
 * Partition values are recorded in the split; after reading, they are backfilled by column name into a full-schema
 * Record, which is then mapped into a Beam Row by [recordToRow]. When there is no residual predicate, only the
 * output columns are projected; when there is a predicate, all table columns are read for the [Evaluator] to
 * evaluate.
 *
 * The Catalog / Table are opened independently in each DoFn instance (created in `@Setup`, closed in `@Teardown`),
 * and marked `@Transient` to keep it serializable.
 *
 * @author wuya
 */
class IcebergReadFileFn(
    private val config: IcebergReadConfig,
    private val schema: Schema,
    private val schemaFields: List<Pair<String, Schema.FieldType>>,
) : DoFn<IcebergFileSplit, Row>() {

    @Transient
    private var catalog: HadoopCatalog? = null

    @Transient
    private var table: Table? = null

    /** The residual evaluator for predicate push-down: evaluates the filter for each row (data columns + partition columns), filtering out non-matching rows. */
    @Transient
    private var evaluator: Evaluator? = null

    @Setup
    fun setup() {
        catalog = IcebergCatalogs.openCatalog(config.warehouse, config.catalogName)
        table = IcebergCatalogs.loadTable(catalog!!, config.table)
        validateReadableSchema(checkNotNull(table).schema(), schemaFields, config.table)
        // The filter has already been handed to TableScan for manifest-level pruning; here we evaluate it once more
        // per row (the Evaluator is based on the full-table schema, so both partition and data columns are judged
        // correctly), ensuring the pushed-down predicate truly takes effect and does not silently skip filtering.
        if (config.filter.isNotBlank()) {
            evaluator = Evaluator(checkNotNull(table).schema().asStruct(), parseIcebergFilter(config.filter), true)
        }
    }

    @Teardown
    fun teardown() {
        runCatching { catalog?.close() }
        catalog = null
    }

    @ProcessElement
    fun processElement(@Element split: IcebergFileSplit, receiver: OutputReceiver<Row>) {
        val t = checkNotNull(table) { "Iceberg table is not initialized" }
        val fullSchema = t.schema()
        val partitionNames = split.partitionNames.toSet()
        val dataSchema = icebergReadProjection(
            fullSchema = fullSchema,
            outputFieldNames = schemaFields.map { it.first }.toSet(),
            partitionNames = partitionNames,
            requiresFilterEvaluation = evaluator != null,
        )

        val inputFile = t.io().newInputFile(split.path)
        val fileFormat = FileFormat.valueOf(split.format)
        // InternalData.read is symmetric with the write side's InternalData.write; it reads Iceberg GenericRecords,
        // and this DoFn handles type conversion and partition backfilling. The reader built here only registers AVRO
        // (consistent with the write side being fixed to AVRO).
        val records: CloseableIterable<Record> = when (fileFormat) {
            FileFormat.AVRO -> org.apache.iceberg.InternalData.read(fileFormat, inputFile)
                .split(split.start, split.length).project(dataSchema).build<Record>()

            else -> throw UnsupportedOperationException(
                "Iceberg data file format [$fileFormat] has no corresponding reader in the current build (only AVRO is supported)",
            )
        }

        records.use { iterable ->
            val it = iterable.iterator()
            while (it.hasNext()) {
                val dataRecord = it.next()
                // Only assign the projected columns; other unread columns stay null, and recordToRow will not access them.
                val full = GenericRecord.create(fullSchema)
                dataSchema.columns().forEach { col ->
                    full.setField(col.name(), dataRecord.getField(col.name()))
                }
                split.partitionNames.forEachIndexed { index, name ->
                    val col = fullSchema.findField(name) ?: return@forEachIndexed
                    full.setField(name, icebergPartitionValue(split.partitionValues[index], col.type()))
                }
                // Residual filtering for predicate push-down: non-matching rows are discarded (both partition and data columns are in full).
                if (evaluator != null && !evaluator!!.eval(full)) continue
                receiver.output(recordToRow(schema, full, schemaFields))
            }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/** Filter evaluation needs the full row; a plain read takes only the output columns, and the split backfills identity partition fields. */
internal fun icebergReadProjection(
    fullSchema: org.apache.iceberg.Schema,
    outputFieldNames: Set<String>,
    partitionNames: Set<String>,
    requiresFilterEvaluation: Boolean,
): org.apache.iceberg.Schema {
    val requiredNames = if (requiresFilterEvaluation) {
        fullSchema.columns().mapTo(linkedSetOf()) { it.name() }
    } else {
        outputFieldNames
    }
    return org.apache.iceberg.Schema(
        fullSchema.columns().filter { it.name() in requiredNames && it.name() !in partitionNames },
    )
}
