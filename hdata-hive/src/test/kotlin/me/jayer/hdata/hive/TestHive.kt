package me.jayer.hdata.hive

import me.jayer.hdata.hive.format.HiveStorageFormat
import me.jayer.hdata.hive.metastore.HiveColumn
import me.jayer.hdata.hive.metastore.HiveTable
import me.jayer.hdata.hive.metastore.InMemoryHiveMetastore
import me.jayer.hdata.hive.metastore.Storage
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger

/**
 * Test fixture: a local temp directory as the warehouse plus an in-process metastore.
 *
 * Together they play the role of the H2 in-memory database in `hdata-jdbc` — they let end-to-end tests really run the whole
 * "metadata -> column directories -> read files by byte range" chain while `mvn test` needs no external service.
 *
 * @author wuya
 */
class TestHive(val name: String = "test-${COUNTER.incrementAndGet()}") : AutoCloseable {

    val warehouse: Path = Files.createTempDirectory("hdata-hive-")
    val metastore: InMemoryHiveMetastore = InMemoryHiveMetastore.named(name)
    val metastoreUri: String = InMemoryHiveMetastore.uriOf(name)

    /** Creates a table and creates its directory. */
    fun createTable(
        table: String,
        format: HiveStorageFormat,
        dataColumns: List<Pair<String, String>>,
        partitionColumns: List<Pair<String, String>> = emptyList(),
        database: String = "default",
        serdeParameters: Map<String, String> = emptyMap(),
        tableParameters: Map<String, String> = emptyMap(),
    ): HiveTable {
        val location = warehouse.resolve("$database.db").resolve(table)
        Files.createDirectories(location)
        val hiveTable = HiveTable(
            databaseName = database,
            tableName = table,
            tableType = HiveTable.MANAGED_TABLE,
            dataColumns = dataColumns.map { HiveColumn(it.first, it.second) },
            partitionColumns = partitionColumns.map { HiveColumn(it.first, it.second) },
            storage = Storage(
                storageFormat = format.toStorageFormat(),
                location = location.toUri().toString().removeSuffix("/"),
                serdeParameters = serdeParameters,
            ),
            parameters = tableParameters,
        )
        metastore.createTable(hiveTable)
        return hiveTable
    }

    /** All data files under the table directory (hidden files and temp directories skipped). */
    fun dataFiles(table: String, database: String = "default"): List<Path> {
        val location = warehouse.resolve("$database.db").resolve(table)
        if (!Files.exists(location)) {
            return emptyList()
        }
        Files.walk(location).use { stream ->
            return stream.filter { Files.isRegularFile(it) }
                .filter { !it.fileName.toString().startsWith('.') && !it.fileName.toString().startsWith('_') }
                .sorted()
                .toList()
        }
    }

    override fun close() {
        metastore.clear()
        Files.walk(warehouse).use { stream ->
            stream.sorted(Comparator.reverseOrder()).forEach { runCatching { Files.delete(it) } }
        }
    }

    private companion object {
        val COUNTER = AtomicInteger()
    }
}
