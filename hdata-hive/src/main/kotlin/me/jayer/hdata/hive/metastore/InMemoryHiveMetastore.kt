package me.jayer.hdata.hive.metastore

import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

/**
 * An in-process metastore, enabled with `metastore_uri: memory://<name>`.
 *
 * It exists for the same reason as the H2 in-memory database in the `hdata-jdbc` tests: to let `mvn test` depend on no external
 * service while **really exercising** the whole chain from metadata to data files — table definition, partition discovery and
 * partition registration all go through the same interface, so switching to [ThriftHiveMetastore] changes not a single line on the read or write side.
 *
 * Instances are cached globally by name: DirectRunner serializes and deserializes DoFns, so what a worker gets is a
 * [HiveMetastoreSpec] rather than this object itself, and only the name can recover the same metadata. This also means it is
 * **only valid within a single JVM**; do not use it on a Flink / Spark cluster.
 *
 * @author wuya
 */
class InMemoryHiveMetastore private constructor(val name: String) : HiveMetastore {

    private val tables = ConcurrentHashMap<String, HiveTable>()

    /** Key is `db.table`, value is partition name -> partition. */
    private val partitions = ConcurrentHashMap<String, MutableMap<String, HivePartition>>()

    override fun getTable(databaseName: String, tableName: String): HiveTable? =
        tables[key(databaseName, tableName)]

    override fun getPartitionNames(databaseName: String, tableName: String): List<String> =
        partitionsOf(databaseName, tableName).keys.sorted()

    /**
     * Only the simplest form is supported: `col = 'value'` joined by `and`.
     *
     * The full filter expression syntax the metastore really supports (`>`, `<`, `like`, `in`) is meaningless in an in-memory
     * implementation: its use case is tests, where a complex filter is better written as explicit partition names. Unknown syntax
     * throws outright; there is no "cannot parse it, so return everything" degradation — that is exactly the mistake the
     */
    override fun getPartitionNamesByFilter(
        databaseName: String,
        tableName: String,
        filter: String,
    ): List<String> {
        val table = getTable(databaseName, tableName) ?: return emptyList()
        val columnNames = table.partitionColumns.map { it.name }
        val wanted = parseFilter(filter)
        wanted.keys.forEach { column ->
            require(column in columnNames) { "$column in the filter is not a partition column of ${table.qualifiedName}: $columnNames" }
        }
        return partitionsOf(databaseName, tableName)
            .filterValues { partition ->
                wanted.all { (column, value) -> partition.values[columnNames.indexOf(column)] == value }
            }
            .keys
            .sorted()
    }

    override fun getPartitionsByNames(
        databaseName: String,
        tableName: String,
        partitionNames: List<String>,
    ): Map<String, HivePartition> {
        val all = partitionsOf(databaseName, tableName)
        return partitionNames.mapNotNull { name -> all[name]?.let { name to it } }.toMap()
    }

    override fun addPartitions(
        databaseName: String,
        tableName: String,
        partitions: Map<String, HivePartition>,
    ): List<String> {
        val all = partitionsOf(databaseName, tableName)
        return partitions.mapNotNull { (name, partition) ->
            if (all.putIfAbsent(name, partition) == null) name else null
        }
    }

    override fun createTable(table: HiveTable) {
        val previous = tables.putIfAbsent(key(table.databaseName, table.tableName), table)
        require(previous == null) { "table already exists: ${table.qualifiedName}" }
        LOGGER.info("in-memory metastore[{}] created table {}", name, table.qualifiedName)
    }

    /** For test fixtures: wipes every table and partition in this instance. */
    fun clear() {
        tables.clear()
        partitions.clear()
    }

    private fun partitionsOf(databaseName: String, tableName: String): MutableMap<String, HivePartition> =
        partitions.computeIfAbsent(key(databaseName, tableName)) { ConcurrentHashMap() }

    private fun key(databaseName: String, tableName: String) = "$databaseName.$tableName"

    private fun parseFilter(filter: String): Map<String, String> = filter
        .split(Regex("(?i)\\s+and\\s+"))
        .filter { it.isNotBlank() }
        .associate { clause ->
            val parts = clause.split('=', limit = 2)
            require(parts.size == 2) { "the in-memory metastore only understands filters of the form `col = 'value'`, got: $clause" }
            parts[0].trim() to parts[1].trim().trim('\'', '"')
        }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(InMemoryHiveMetastore::class.java)

        private val INSTANCES = ConcurrentHashMap<String, InMemoryHiveMetastore>()

        fun named(name: String): InMemoryHiveMetastore {
            require(name.isNotBlank()) { "memory:// must be followed by a name, for example memory://test" }
            return INSTANCES.computeIfAbsent(name, ::InMemoryHiveMetastore)
        }

        /** A uri of the form `memory://<name>`, for use in configs. */
        fun uriOf(name: String): String = "${HiveMetastores.MEMORY_SCHEME}$name"
    }
}
