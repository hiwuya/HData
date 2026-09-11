package me.jayer.hdata.hive

import me.jayer.hdata.hive.format.ConfigPredicate
import me.jayer.hdata.hive.metastore.HiveMetastoreSpec
import java.io.Serializable

/**
 * Config of `ReadFromHive`.
 *
 * ```yaml
 * - type: ReadFromHive
 *   config:
 *     metastore_uri: "thrift://localhost:9083"
 *     database: default
 *     table: t_order
 *     partition_filter: "dt = '2024-01-01'"   # or pin the partition names down with partitions
     *     columns: [id, name, dt]                 # leave empty to read all columns
     *     predicates:                            # read-side predicate pushdown (mirrors Trino's TupleDomain)
     *       - column: id
     *         op: ">"                            # = != > >= < <= is null is not null
     *         value: "1000"
     *     hadoop_conf:
     *       fs.defaultFS: "hdfs://nameservice1"
     * ```
     *
 * This connects to the **metastore** (9083 by default), not to HiveServer2 (10000).
 * Before the refactor it went through `jdbc:hive2://`, where reading one partition made HiveServer2 start an MR/Tez job
 * that serialized the data into a result set and shipped it back row by row; now we take the metadata and
 * **read the files under the table directory directly**, the same path Trino takes when reading Hive.
     *
 * `predicates` is an optional read-side filter: it only pushes down simple comparisons on **data columns** (ANDed),
 * using the column statistics (min/max) of ORC stripes / Parquet row groups to skip chunks that cannot match,
 * with a row-level filter as a safety net to keep the result correct. Partition-level pruning still goes through `partition_filter`.
     *
     * @author wuya
     */
    data class HiveReadConfig(
        val metastoreUri: String = "",
        val database: String = "default",
        val table: String = "",
        /** Explicit partition name, e.g. `dt=2024-01-01/hr=01`. Use either this or [partitionFilter]. */
        val partitions: List<String> = emptyList(),
        /** Partition filter expression for the metastore, e.g. `dt = '2024-01-01'`. */
        val partitionFilter: String = "",
        /** Read only these columns (partition columns included); leave empty to read all. */
        val columns: List<String> = emptyList(),
        /** Read-side predicate pushdown (ANDed). The column must be a data or partition column of a numeric/string type. */
        val predicates: List<ConfigPredicate> = emptyList(),
        /**
         * Maximum number of rows to output (Trino's `LIMIT` equivalent). `<= 0` means unlimited.
         *
         * Note: with parallel readers we cannot "stop all IO globally once N rows are scanned" (only Trino's single-node coordinator can),
         * so this is pushed down as a `Take` on the output — at most N rows and semantically correct, but the source still scans the whole table.
         */
        val limit: Long = -1,
        /**
         * Sampling pushdown (Trino's `TABLESAMPLE BERNOULLI` equivalent): each row is kept with probability [SampleConfig.fraction],
         * inside the reader that does the row-level filtering, so dropped rows never reach downstream (a real pushdown, it shrinks the downstream data).
         */
        val sample: SampleConfig? = null,
        /**
         * Aggregation pushdown (Trino's aggregation pushdown equivalent): `count(*)` / `min(col)` / `max(col)`
         * read the column statistics in the ORC/Parquet file tail without scanning rows. Mutually exclusive with predicates / limit / sample (the result could not be derived from statistics otherwise).
         */
        val aggregates: List<ConfigAggregate> = emptyList(),
        /** Whether to recurse when a partition directory contains subdirectories. Mirrors Hive's `hive.mapred.supports.subdirectories`. */
        val recursiveDirectories: Boolean = false,
        /** Passed through to the Hadoop `Configuration`, e.g. `fs.defaultFS` or object store access keys. */
        val hadoopConf: Map<String, String> = emptyMap(),
        /** Socket timeout of the metastore. */
        val metastoreTimeoutMillis: Int = 60_000,
        /** Maximum number of bytes in one split; only effective for splittable formats. */
        val splitBytes: Long = DEFAULT_SPLIT_BYTES,
    ) : Serializable {

        fun validate() {
            require(metastoreUri.isNotBlank()) { "metastore_uri must not be blank" }
            require(database.isNotBlank()) { "database must not be blank" }
            require(table.isNotBlank()) { "table must not be blank" }
            require(metastoreTimeoutMillis > 0) { "metastore_timeout_millis must be > 0" }
            require(partitions.isEmpty() || partitionFilter.isBlank()) {
                "partitions and partition_filter are mutually exclusive"
            }
            require(splitBytes > 0) { "split_bytes must be > 0" }
            require(partitions.none { it.isBlank() }) { "partitions must not contain a blank partition name" }
            require(partitions.distinct().size == partitions.size) { "partitions must not contain duplicates, otherwise the same partition would be read more than once" }
            require(columns.none { it.isBlank() }) { "columns must not contain a blank column name" }
            require(columns.distinct().size == columns.size) { "columns must not contain duplicates" }
            require(hadoopConf.keys.none { it.isBlank() }) { "hadoop_conf must not contain a blank key" }
            val knownOps = setOf("=", "==", "eq", "!=", "<>", "neq", ">", "gt", ">=", "gte", "ge", "<", "lt", "<=", "lte", "le", "is null", "isnull", "is not null", "isnotnull")
            predicates.forEach { p ->
                require(p.column.isNotBlank()) { "a predicate in predicates is missing column" }
                val op = p.op.trim().lowercase()
                require(op in knownOps) { "the operator [$op] of column [${p.column}] in predicates is not supported" }
                val isNullOp = op == "is null" || op == "isnull" || op == "is not null" || op == "isnotnull"
                if (!isNullOp) {
                    require(p.value.isNotBlank()) { "the comparison value of column [${p.column}] in predicates must not be blank" }
                }
            }
            require(limit > 0 || limit == -1L) { "limit must be > 0 (or leave it empty / pass -1 for unlimited)" }
            sample?.let { s ->
                require(s.fraction > 0.0 && s.fraction <= 1.0) { "sample.fraction must be within (0, 1]" }
                SampleMethod.of(s.method) // an illegal method fails explicitly instead of degrading silently
            }
            val knownAggTypes = setOf("count", "min", "max", "sum", "avg")
            aggregates.forEach { a ->
                val type = a.type.trim().lowercase()
                require(type in knownAggTypes) {
                    "type [${a.type}] in aggregates is not supported, choose from count / min / max / sum / avg"
                }
                if (type == "count") {
                    require(a.column.isBlank() || a.column.trim() == "*") {
                        "count in aggregates can only omit column or use *"
                    }
                } else {
                    require(a.column.isNotBlank()) { "[${a.type}] in aggregates must specify a column" }
                }
            }
            if (aggregates.isNotEmpty()) {
                require(columns.isEmpty()) { "aggregation pushdown mode does not use columns, please remove it from the config" }
                require(predicates.isEmpty()) { "aggregation pushdown mode does not use predicates, please remove it from the config" }
                require(limit == -1L) { "aggregation pushdown mode does not use limit, please remove it from the config" }
                require(sample == null) { "aggregation pushdown mode does not use sample, please remove it from the config" }
                require(splitBytes == DEFAULT_SPLIT_BYTES) {
                    "aggregation pushdown mode works on file statistics and does not use split_bytes, please remove it from the config"
                }
                val outputNames = aggregates.map { aggregate ->
                    val type = aggregate.type.trim().lowercase()
                    if (type == "count") "count" else "${type}_${aggregate.column.trim().lowercase()}"
                }
                require(outputNames.distinct().size == outputNames.size) {
                    "aggregates output column names must not be duplicated: $outputNames"
                }
            }
        }

    fun metastoreSpec(): HiveMetastoreSpec = HiveMetastoreSpec(metastoreUri, metastoreTimeoutMillis, hadoopConf)

    val qualifiedTable: String get() = "$database.$table"

    companion object {
        private const val serialVersionUID: Long = 1
        const val DEFAULT_SPLIT_BYTES: Long = 64L * 1024 * 1024
    }
}

/** Sampling pushdown config of `ReadFromHive` (Trino's `TABLESAMPLE` equivalent). */
data class SampleConfig(
    /** Probability of each row being kept, must be within (0, 1]. */
    val fraction: Double = 1.0,
    /** Sampling method: `bernoulli` (row by row, the default) or `system` (skip whole stripes/row groups, less IO). */
    val method: String = "bernoulli",
    /** Random seed; without it every run produces a different result. */
    val seed: Long? = null,
) : Serializable

/** Aggregation pushdown config of `ReadFromHive` (Trino's `count` / `min` / `max` equivalent). */
data class ConfigAggregate(
    /** `count` / `min` / `max`; `min`/`max` must be configured with `column`. */
    val type: String = "",
    /** The column `min`/`max` apply to; ignored by `count`. */
    val column: String = "",
) : Serializable

/** Sampling methods, mirroring the two `TABLESAMPLE` forms in Trino. */
enum class SampleMethod {
    /** Keeps rows at random, Trino's `TABLESAMPLE BERNOULLI` equivalent. */
    BERNOULLI,

    /**
     * Skips whole storage blocks (ORC stripe / Parquet row group), Trino's `TABLESAMPLE SYSTEM` equivalent.
     * Less IO (whole chunks stay unread) but the granularity is a block; non-splittable formats degrade to row by row.
     */
    SYSTEM,

    ;

    companion object {
        fun of(name: String): SampleMethod = entries.firstOrNull { it.name.equals(name.trim(), ignoreCase = true) }
            ?: throw IllegalArgumentException(
                "unrecognized sample.method: $name, choose from: ${entries.joinToString { it.name.lowercase() }}"
            )
    }
}

/**
 * What to do when data already exists; mirrors Hive's `INSERT INTO` / `INSERT OVERWRITE`
 * and Trino's `hive.insert-existing-partitions-behavior`.
 */
enum class HiveWriteMode {
    /** `INSERT INTO`: new files are added and existing files are left untouched. */
    APPEND,

    /**
     * `INSERT OVERWRITE`: the old files in **the partitions actually written this run** are deleted, leaving only what this run wrote.
     *
     * The semantics match Hive's dynamic partition overwrite: partitions that receive no data are **left alone**.
     * To empty a whole table, `DROP` it yourself or overwrite the partitions explicitly — a sync tool should not do something irreversible for the user.
     */
    OVERWRITE,
    ;

    companion object {
        fun of(name: String): HiveWriteMode = entries.firstOrNull { it.name.equals(name.trim(), ignoreCase = true) }
            ?: throw IllegalArgumentException(
                "unrecognized write_mode: $name, choose from: ${entries.joinToString { it.name.lowercase() }}"
            )
    }
}

/**
 * Config of `WriteToHive`.
 *
 * ```yaml
 * - type: WriteToHive
 *   config:
 *     metastore_uri: "thrift://localhost:9083"
 *     database: default
 *     table: t_order
 *     write_mode: overwrite     # append is the default
 *     create_partitions: true
 * ```
 *
 * The target table must **already exist** — just like Trino's `INSERT INTO`, creating a table is DDL, not a sync job's business.
 * The storage format, location and SerDe parameters all come from the metastore, so the files written match what Hive itself writes.
 *
 * Partitioning is **dynamic**: each row decides which partition it lands in from its own partition column values, and one job can write
 * any number of partitions, matching Hive's dynamic partition insert. When the upstream has no partition column, add a constant column with `MapToFields`.
 *
 * @author wuya
 */
data class HiveWriteConfig(
    val metastoreUri: String = "",
    val database: String = "default",
    val table: String = "",
    /** `append` (the default, `INSERT INTO` equivalent) or `overwrite` (`INSERT OVERWRITE` equivalent). */
    val writeMode: String = "append",
    /** Register newly created partitions in the metastore after writing. Turn this off and Hive cannot query the data of new partitions. */
    val createPartitions: Boolean = true,
    /** Number of output shards; 0 lets the runner decide. */
    val numShards: Int = 0,
    /** File name prefix, handy for telling which job wrote a file. */
    val filePrefix: String = "part",
    val hadoopConf: Map<String, String> = emptyMap(),
    val metastoreTimeoutMillis: Int = 60_000,
) : Serializable {

    fun validate() {
        require(metastoreUri.isNotBlank()) { "metastore_uri must not be blank" }
        require(database.isNotBlank()) { "database must not be blank" }
        require(table.isNotBlank()) { "table must not be blank" }
        require(numShards >= 0) { "num_shards must not be negative" }
        require(filePrefix.isNotBlank()) { "file_prefix must not be blank" }
        require(metastoreTimeoutMillis > 0) { "metastore_timeout_millis must be > 0" }
        mode()
    }

    fun mode(): HiveWriteMode = HiveWriteMode.of(writeMode)

    fun metastoreSpec(): HiveMetastoreSpec = HiveMetastoreSpec(metastoreUri, metastoreTimeoutMillis, hadoopConf)

    val qualifiedTable: String get() = "$database.$table"

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
