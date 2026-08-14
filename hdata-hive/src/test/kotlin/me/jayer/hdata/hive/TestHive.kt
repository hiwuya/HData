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
 * 测试夹具：一个本地临时目录当仓库 + 一个进程内 metastore。
 *
 * 这一对东西合起来就是 `hdata-jdbc` 里 H2 内存库的角色——让端到端测试真的跑完
 * "元数据 -> 列目录 -> 按字节区间读文件"这条链路，同时 `mvn test` 不需要任何外部服务。
 *
 * @author wuya
 */
class TestHive(val name: String = "test-${COUNTER.incrementAndGet()}") : AutoCloseable {

    val warehouse: Path = Files.createTempDirectory("hdata-hive-")
    val metastore: InMemoryHiveMetastore = InMemoryHiveMetastore.named(name)
    val metastoreUri: String = InMemoryHiveMetastore.uriOf(name)

    /** 建一张表并把目录创建出来。 */
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

    /** 表目录下的全部数据文件（跳过隐藏文件与临时目录）。 */
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
