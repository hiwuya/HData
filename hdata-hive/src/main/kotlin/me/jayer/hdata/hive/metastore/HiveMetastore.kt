package me.jayer.hdata.hive.metastore

import java.io.Serializable

/**
 * Abstraction of metadata access, shaped after Trino's `HiveMetastore`.
 *
 * Splitting out an interface has real benefits, it is not layering for its own sake:
 *  - in production it is [ThriftHiveMetastore], speaking the metastore's thrift protocol directly, without HiveServer2;
 *  - in tests it is [InMemoryHiveMetastore], so `mvn test` needs no external service;
 *  - to support Glue / DLF / a file-based metastore later, add one implementation and neither the read nor the write side changes.
 *
 * The implementation must be **created once per worker** (built in `@Setup`, closed in `@Teardown`); do not try to serialize the
 * implementation itself and ship it with a DoFn — a thrift socket is not serializable.
 * Only [HiveMetastoreSpec] is serializable.
 *
 * @author wuya
 */
interface HiveMetastore : AutoCloseable {

    /** Returns null instead of throwing when the table does not exist. */
    fun getTable(databaseName: String, tableName: String): HiveTable?

    /** All partition names, e.g. `dt=2024-01-01/hr=01`. Returns an empty list for a non-partitioned table. */
    fun getPartitionNames(databaseName: String, tableName: String): List<String>

    /**
     * Takes partition names by the metastore's partition filter expression, e.g. `dt = "2024-01-01"`.
     *
     * Hand it to the metastore rather than fetching all partition names and filtering client-side: on a table with tens of
     * thousands of partitions one `get_partition_names` response is already several MB.
     */
    fun getPartitionNamesByFilter(databaseName: String, tableName: String, filter: String): List<String>

    /** Fetches partition details in bulk, indexed by partition name; partitions that do not exist are absent from the result. */
    fun getPartitionsByNames(
        databaseName: String,
        tableName: String,
        partitionNames: List<String>,
    ): Map<String, HivePartition>

    /**
     * Registers partitions. Existing ones are **skipped rather than reported as an error** — the write side may rerun, or may
     *
     * @return the partition names that were really created
     */
    fun addPartitions(databaseName: String, tableName: String, partitions: Map<String, HivePartition>): List<String>

    /** Creates a table. Mainly for tests and for "create the target table when it is missing" scenarios. */
    fun createTable(table: HiveTable)

    override fun close() {}
}

/**
 * Serializable metastore connection declaration, shipped to workers with the DoFn and turned back into a client by [HiveMetastores.create].
 *
 * @param uri `thrift://host:9083` (several may be comma-separated for HA), or `memory://<name>` (in-process, for tests)
 * @param timeoutMillis socket read/write timeout
 * @param configuration key/values passed through to the Hadoop `Configuration`, e.g. `fs.defaultFS`
 */
data class HiveMetastoreSpec(
    val uri: String,
    val timeoutMillis: Int = 60_000,
    val configuration: Map<String, String> = emptyMap(),
) : Serializable {
    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * Picks an implementation by the scheme of [HiveMetastoreSpec.uri].
 */
object HiveMetastores {

    const val THRIFT_SCHEME = "thrift://"
    const val MEMORY_SCHEME = "memory://"

    fun create(spec: HiveMetastoreSpec): HiveMetastore {
        val uri = spec.uri.trim()
        return when {
            uri.startsWith(MEMORY_SCHEME) -> InMemoryHiveMetastore.named(uri.removePrefix(MEMORY_SCHEME))
            uri.startsWith(THRIFT_SCHEME) -> ThriftHiveMetastore.connect(spec)
            else -> throw IllegalArgumentException(
                "unrecognized metastore_uri: ${spec.uri}, expected thrift://host:9083 or memory://<name>"
            )
        }
    }

    /** Creates a temporary client to run a piece of logic and closes it right after; used to fetch metadata at graph construction. */
    fun <T> withMetastore(spec: HiveMetastoreSpec, block: (HiveMetastore) -> T): T =
        create(spec).use(block)
}
