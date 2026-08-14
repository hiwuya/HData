package me.jayer.hdata.hive.metastore

import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

/**
 * 进程内的 metastore，`metastore_uri: memory://<名字>` 时启用。
 *
 * 存在的理由和 `hdata-jdbc` 测试里的 H2 内存库一样：让 `mvn test` 不依赖任何外部服务，
 * 同时**真的跑通**从元数据到数据文件的整条链路——表定义、分区发现、分区注册全都走同一套接口，
 * 换成 [ThriftHiveMetastore] 时读写两端一行都不用改。
 *
 * 实例按名字全局缓存：DirectRunner 会把 DoFn 序列化再反序列化，worker 侧拿到的是
 * [HiveMetastoreSpec] 而不是这个对象本身，靠名字才能找回同一份元数据。
 * 这也意味着它**只在单个 JVM 内有效**，不要在 Flink / Spark 集群上用。
 *
 * @author wuya
 */
class InMemoryHiveMetastore private constructor(val name: String) : HiveMetastore {

    private val tables = ConcurrentHashMap<String, HiveTable>()

    /** key 是 `db.table`，value 是分区名 -> 分区。 */
    private val partitions = ConcurrentHashMap<String, MutableMap<String, HivePartition>>()

    override fun getTable(databaseName: String, tableName: String): HiveTable? =
        tables[key(databaseName, tableName)]

    override fun getPartitionNames(databaseName: String, tableName: String): List<String> =
        partitionsOf(databaseName, tableName).keys.sorted()

    /**
     * 只支持 `col = 'value'` 用 `and` 连起来的最简形式。
     *
     * metastore 真正支持的过滤表达式语法（`>`、`<`、`like`、`in`）在内存实现里没有意义：
     * 用它的场景是测试，写复杂过滤条件不如直接写分区名。语法不认识就直接抛，
     * 不做"看不懂就返回全部"这种静默降级——那正是重构前 `SHOW PARTITIONS` 吞异常犯的错。
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
            require(column in columnNames) { "过滤条件里的 $column 不是 ${table.qualifiedName} 的分区列: $columnNames" }
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
        require(previous == null) { "表已存在: ${table.qualifiedName}" }
        LOGGER.info("内存 metastore[{}] 建表 {}", name, table.qualifiedName)
    }

    /** 测试夹具用：清掉这个实例里的全部表与分区。 */
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
            require(parts.size == 2) { "内存 metastore 只认 `col = 'value'` 形式的过滤条件，收到: $clause" }
            parts[0].trim() to parts[1].trim().trim('\'', '"')
        }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(InMemoryHiveMetastore::class.java)

        private val INSTANCES = ConcurrentHashMap<String, InMemoryHiveMetastore>()

        fun named(name: String): InMemoryHiveMetastore {
            require(name.isNotBlank()) { "memory:// 后面要带一个名字，例如 memory://test" }
            return INSTANCES.computeIfAbsent(name, ::InMemoryHiveMetastore)
        }

        /** `memory://<名字>` 形式的 uri，写配置时用。 */
        fun uriOf(name: String): String = "${HiveMetastores.MEMORY_SCHEME}$name"
    }
}
