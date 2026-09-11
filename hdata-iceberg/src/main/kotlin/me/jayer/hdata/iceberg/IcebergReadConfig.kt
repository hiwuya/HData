package me.jayer.hdata.iceberg

import me.jayer.hdata.iceberg.internal.parseSchemaFields
import org.apache.beam.sdk.schemas.Schema
import java.io.Serializable

/**
 * Iceberg connection info (shared): warehouse directory + catalog name + table name.
 *
 * @author wuya
 */
interface IcebergConnectionConfig : Serializable {
    val warehouse: String
    val catalogName: String
    val table: String
}

/**
 * Config for `ReadFromIceberg`.
 *
 * The read side **can be graphed without connecting to the database**: the output schema is declared via
 * `schema_fields` (`name:TYPE`); at runtime the table is loaded from the HadoopCatalog under `warehouse`, scanned with
 * IcebergGenerics, and mapped row by row.
 *
 * @author wuya
 */
data class IcebergReadConfig(
    override val warehouse: String,
    override val catalogName: String = "hdata",
    override val table: String,
    /** The output schema for a plain row-by-row read; in aggregation mode it is derived from the table metadata, so it must be left empty. */
    val schemaFields: List<String> = emptyList(),
    /**
     * The target size (in bytes) for further splitting within a single data file. Files larger than this are cut into
     * multiple splits read in parallel, with parallelism coming from row-group / sync-block granularity; the default
     * is 128MB, aligned with Iceberg's default split size.
     */
    val splitSize: Long = DEFAULT_SPLIT_SIZE,
    /**
     * Filter push-down (SQL-like WHERE): `age >= 40 AND name = 'bob'`, `id IN (1, 2, 3)`, `age IS NOT NULL`.
     * Pushed down to Iceberg's TableScan for manifest-level pruning (non-matching splits are cut away at
     * partition/file granularity), and the read side then evaluates the residual predicate per row with `Evaluator`,
     * so both data columns and partition columns are filtered correctly.
     */
    val filter: String = "",
    /**
     * The maximum number of rows to read; `-1` means unlimited. Iceberg has no native global LIMIT, so when limiting
     * rows it degrades to a single worker that reads sequentially across files over the current snapshot and stops
     * after actually producing `limit` matching records.
     */
    val limit: Long = -1,
    /**
     * Push-down aggregation: `["count", "min:age", "max:age", "sum:amount", "avg:amount"]`. COUNT comes from the data
     * files' metadata `recordCount`; MIN/MAX/SUM/AVG project the relevant column, accumulate file by file, then merge
     * globally into a single row. When non-empty, a single aggregated row is output and the schema is derived directly
     * from the Iceberg table metadata, so [schemaFields] must be left empty; it is mutually exclusive with [limit] —
     * aggregation has global semantics, so limit is meaningless and configuring both errors out.
     */
    val aggregations: List<String> = emptyList(),
) : IcebergConnectionConfig {

    fun validate() {
        require(warehouse.isNotBlank()) { "warehouse must not be empty" }
        require(catalogName.isNotBlank()) { "catalog_name must not be empty" }
        require(table.isNotBlank()) { "table must not be empty" }
        require(splitSize > 0) { "split_size must be > 0" }
        require(limit == -1L || limit > 0) { "limit must be > 0 (or left empty / set to -1 for unlimited)" }
        if (filter.isNotBlank()) parseIcebergFilter(filter) // A parse failure errors out at graph-construction time.
        if (aggregations.isNotEmpty()) {
            require(limit == -1L) { "aggregations mode does not use limit, please remove it from the config" }
            require(schemaFields.isEmpty()) { "aggregations mode does not use schema_fields, please remove it from the config" }
            require(splitSize == DEFAULT_SPLIT_SIZE) {
                "aggregations mode aggregates per data file and does not use split_size, please remove it from the config"
            }
            parseAggregations(aggregations)
        } else {
            require(schemaFields.isNotEmpty()) { "a plain read requires schema_fields" }
            if (limit > 0) {
                require(splitSize == DEFAULT_SPLIT_SIZE) {
                    "limit mode forces a single-worker sequential read and does not use split_size, please remove it from the config"
                }
            }
        }
        val fields = parseSchemaFields(schemaFields)
        require(fields.map { it.first }.distinct().size == fields.size) { "schema_fields field names must not be duplicated" }
    }

    fun outputSchema(): Schema = Schema.builder().apply {
        parseSchemaFields(schemaFields).forEach { (name, type) -> addNullableField(name, type) }
    }.build()

    companion object {
        private const val serialVersionUID: Long = 1
        const val DEFAULT_SPLIT_SIZE: Long = 128L * 1024 * 1024
    }
}
