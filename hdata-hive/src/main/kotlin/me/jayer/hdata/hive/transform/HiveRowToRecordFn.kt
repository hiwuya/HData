package me.jayer.hdata.hive.transform

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.hive.metastore.HiveColumn
import me.jayer.hdata.hive.metastore.PartitionNames
import me.jayer.hdata.hive.type.HiveValues
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.TupleTag
import org.slf4j.LoggerFactory

/**
 * Splits an upstream row into "partition name + data row", mirroring the step in Trino's `HivePageSink` that computes the
 * partition key.
 *
 * Two easy-to-get-wrong things, spelled out once here:
 *
 *  1. **There are no partition columns in the data file**. A Hive partition value only exists in the directory name
 *     `dt=2024-01-01`; writing it into the file would instead make Hive read misaligned columns. So partition columns are
 *     stripped from the row here, leaving only the data columns.
 *  2. **Align by column name, not by index**. The upstream Row comes from an arbitrary transform, so its field order matching the Hive table is pure
 *     coincidence; writing by index silently puts data into the wrong column — impossible to notice before it shows up downstream.
 * @author wuya
 */
class HiveRowToRecordFn(
    private val fileSchema: Schema,
    private val partitionColumns: List<HiveColumn>,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
    private val errorTag: TupleTag<Row>,
) : DoFn<Row, KV<String, Row>>() {

    @ProcessElement
    fun processElement(@Element row: Row, context: ProcessContext) {
        try {
            context.output(KV.of(partitionName(row), HiveValues.align(row, fileSchema)))
        } catch (e: Exception) {
            if (!deadLetter) {
                throw e
            }
            LOGGER.warn("row cannot be written to Hive, sending it to the dead letter: {}", e.message)
            context.output(errorTag, ErrorSchemas.failure(errorSchema, row, e, transformName))
        }
    }

    /** A non-partitioned table returns an empty string, which [me.jayer.hdata.hive.HiveWriteProvider] treats as
     * writing directly into the table directory. */
    private fun partitionName(row: Row): String {
        if (partitionColumns.isEmpty()) {
            return ""
        }
        val values = partitionColumns.map { column ->
            val field = row.schema.fields.firstOrNull { it.name.equals(column.name, ignoreCase = true) }
            requireNotNull(field) { "the upstream data has no partition column ${column.name}, cannot tell which partition this row belongs to" }
            HiveValues.toPartitionLiteral(row.getValue<Any?>(field.name))
        }
        return PartitionNames.makePartName(partitionColumns.map { it.name }, values)
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(HiveRowToRecordFn::class.java)
    }
}
