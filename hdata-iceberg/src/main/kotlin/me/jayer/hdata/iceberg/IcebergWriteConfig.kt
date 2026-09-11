package me.jayer.hdata.iceberg

import java.io.Serializable

/** How to handle existing data. */
enum class IcebergWriteMode {

    /** New data files are appended; the table's existing data is left untouched. */
    APPEND,

    /** Clear the table first (as one atomic commit), then write this run's data. */
    OVERWRITE,
    ;

    companion object {
        fun of(name: String): IcebergWriteMode = entries.firstOrNull { it.name.equals(name.trim(), ignoreCase = true) }
            ?: throw IllegalArgumentException(
                "Invalid write_mode value: $name, valid options: ${entries.joinToString { it.name.lowercase() }}"
            )
    }
}

/**
 * Config for `WriteToIceberg`.
 *
 * On write, the target table structure is declared via `schema_fields`; the table is auto-created if it does not
 * exist (unpartitioned). Each bundle's accumulated rows are written into one data file and then `append`ed.
 * `write_mode: overwrite` clears the table once **before all writes**, see
 * [me.jayer.hdata.iceberg.transform.IcebergTruncateFn]. `hadoop_conf` passes raw Hadoop `Configuration` overrides
 * through to the `HadoopCatalog` — for example `fs.s3a.endpoint` to run the warehouse on an S3-compatible store.
 *
 * @author wuya
 */
data class IcebergWriteConfig(
    override val warehouse: String,
    override val catalogName: String = "hdata",
    override val table: String,
    override val hadoopConf: Map<String, String> = emptyMap(),
    val schemaFields: List<String>,
    val writeMode: String = "append",
) : IcebergConnectionConfig {

    fun validate() {
        require(warehouse.isNotBlank()) { "warehouse must not be empty" }
        require(catalogName.isNotBlank()) { "catalog_name must not be empty" }
        require(table.isNotBlank()) { "table must not be empty" }
        require(hadoopConf.keys.none { it.isBlank() }) { "hadoop_conf must not contain a blank key" }
        require(schemaFields.isNotEmpty()) { "schema_fields must not be empty" }
        val fields = me.jayer.hdata.iceberg.internal.parseSchemaFields(schemaFields)
        require(fields.map { it.first }.distinct().size == fields.size) { "schema_fields field names must not be duplicated" }
        mode()
    }

    fun mode(): IcebergWriteMode = IcebergWriteMode.of(writeMode)

    fun outputSchema() = me.jayer.hdata.iceberg.internal.schemaOf(schemaFields)

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
