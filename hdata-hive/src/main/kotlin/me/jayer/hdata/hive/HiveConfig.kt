package me.jayer.hdata.hive

import me.jayer.hdata.hive.metastore.HiveMetastoreSpec
import java.io.Serializable

/**
 * `ReadFromHive` 的配置。
 *
 * ```yaml
 * - type: ReadFromHive
 *   config:
 *     metastore_uri: "thrift://localhost:9083"
 *     database: default
 *     table: t_order
 *     partition_filter: "dt = '2024-01-01'"   # 或者用 partitions 写死分区名
 *     columns: [id, name, dt]                 # 留空读全部
 *     hadoop_conf:
 *       fs.defaultFS: "hdfs://nameservice1"
 * ```
 *
 * 这里连的是 **metastore**（默认 9083），不是 HiveServer2（10000）。
 * 重构前走的是 `jdbc:hive2://`，每读一个分区就让 HiveServer2 起一个 MR/Tez 作业把数据
 * 序列化成结果集再一行行传回来；现在是拿到元数据后**直接读表目录下的文件**，
 * 和 Trino 读 Hive 是同一条路。
 *
 * @author wuya
 */
data class HiveReadConfig(
    val metastoreUri: String = "",
    val database: String = "default",
    val table: String = "",
    /** 显式分区名，形如 `dt=2024-01-01/hr=01`。与 [partitionFilter] 二选一。 */
    val partitions: List<String> = emptyList(),
    /** metastore 的分区过滤表达式，形如 `dt = '2024-01-01'`。 */
    val partitionFilter: String = "",
    /** 只读这些列（含分区列）；留空读全部。 */
    val columns: List<String> = emptyList(),
    /** 分区目录下还有子目录时是否递归。对应 Hive 的 `hive.mapred.supports.subdirectories`。 */
    val recursiveDirectories: Boolean = false,
    /** 透传给 Hadoop `Configuration`，例如 `fs.defaultFS`、对象存储的 ak/sk。 */
    val hadoopConf: Map<String, String> = emptyMap(),
    /** metastore 的 socket 超时。 */
    val metastoreTimeoutMillis: Int = 60_000,
    /** 一个分片最多多少字节，只对可切分的格式有效。 */
    val splitBytes: Long = 64L * 1024 * 1024,
) : Serializable {

    fun validate() {
        require(metastoreUri.isNotBlank()) { "metastore_uri 不能为空" }
        require(database.isNotBlank()) { "database 不能为空" }
        require(table.isNotBlank()) { "table 不能为空" }
        require(metastoreTimeoutMillis > 0) { "metastore_timeout_millis 必须 > 0" }
        require(partitions.isEmpty() || partitionFilter.isBlank()) {
            "partitions 与 partition_filter 只能配一个"
        }
        require(splitBytes > 0) { "split_bytes 必须 > 0" }
        require(partitions.none { it.isBlank() }) { "partitions 不能包含空分区名" }
        require(columns.none { it.isBlank() }) { "columns 不能包含空列名" }
        require(columns.distinct().size == columns.size) { "columns 不能重复" }
    }

    fun metastoreSpec(): HiveMetastoreSpec = HiveMetastoreSpec(metastoreUri, metastoreTimeoutMillis, hadoopConf)

    val qualifiedTable: String get() = "$database.$table"

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * 已有数据存在时怎么处理，对应 Hive 的 `INSERT INTO` / `INSERT OVERWRITE`，
 * 也对应 Trino 的 `hive.insert-existing-partitions-behavior`。
 */
enum class HiveWriteMode {
    /** `INSERT INTO`：新文件加进去，原有文件原样保留。 */
    APPEND,

    /**
     * `INSERT OVERWRITE`：**本次写到的那些分区**里的旧文件删掉，只留这次写出来的。
     *
     * 语义与 Hive 的动态分区覆盖一致：没有数据落到的分区**不动**。
     * 想清空整张表要自己 `DROP` 或者按分区显式覆盖——同步工具不该替用户做这种不可逆的事。
     */
    OVERWRITE,
    ;

    companion object {
        fun of(name: String): HiveWriteMode = entries.firstOrNull { it.name.equals(name.trim(), ignoreCase = true) }
            ?: throw IllegalArgumentException(
                "无法识别的 write_mode: $name，可选: ${entries.joinToString { it.name.lowercase() }}"
            )
    }
}

/**
 * `WriteToHive` 的配置。
 *
 * ```yaml
 * - type: WriteToHive
 *   config:
 *     metastore_uri: "thrift://localhost:9083"
 *     database: default
 *     table: t_order
 *     write_mode: overwrite     # 默认 append
 *     create_partitions: true
 * ```
 *
 * 目标表必须**已经存在**——和 Trino 的 `INSERT INTO` 一样，建表是 DDL，不是同步作业该干的事。
 * 表的存储格式、目录、SerDe 参数全部从 metastore 读，写出来的文件因此和 Hive 自己写的一致。
 *
 * 分区是**动态**的：每一行按自己的分区列取值决定落到哪个分区，一次作业可以写出任意多个分区，
 * 与 Hive 的动态分区插入一致。上游没有分区列时，用 `MapToFields` 补一个常量列即可。
 *
 * @author wuya
 */
data class HiveWriteConfig(
    val metastoreUri: String = "",
    val database: String = "default",
    val table: String = "",
    /** `append`（默认，等价 `INSERT INTO`）或 `overwrite`（等价 `INSERT OVERWRITE`）。 */
    val writeMode: String = "append",
    /** 写完之后把新出现的分区注册进 metastore。关掉的话新分区的数据 Hive 是查不到的。 */
    val createPartitions: Boolean = true,
    /** 落盘分片数，0 表示交给 runner 决定。 */
    val numShards: Int = 0,
    /** 文件名前缀，方便识别是哪个作业写的。 */
    val filePrefix: String = "part",
    val hadoopConf: Map<String, String> = emptyMap(),
    val metastoreTimeoutMillis: Int = 60_000,
) : Serializable {

    fun validate() {
        require(metastoreUri.isNotBlank()) { "metastore_uri 不能为空" }
        require(database.isNotBlank()) { "database 不能为空" }
        require(table.isNotBlank()) { "table 不能为空" }
        require(numShards >= 0) { "num_shards 不能为负" }
        require(filePrefix.isNotBlank()) { "file_prefix 不能为空" }
        require(metastoreTimeoutMillis > 0) { "metastore_timeout_millis 必须 > 0" }
        mode()
    }

    fun mode(): HiveWriteMode = HiveWriteMode.of(writeMode)

    fun metastoreSpec(): HiveMetastoreSpec = HiveMetastoreSpec(metastoreUri, metastoreTimeoutMillis, hadoopConf)

    val qualifiedTable: String get() = "$database.$table"

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
