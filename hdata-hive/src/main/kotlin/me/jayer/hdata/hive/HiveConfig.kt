package me.jayer.hdata.hive

import me.jayer.hdata.hive.format.ConfigPredicate
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
     *     predicates:                            # 读取端谓词下推（对齐 Trino 的 TupleDomain）
     *       - column: id
     *         op: ">"                            # = != > >= < <= is null is not null
     *         value: "1000"
     *     hadoop_conf:
     *       fs.defaultFS: "hdfs://nameservice1"
     * ```
     *
     * 这里连的是 **metastore**（默认 9083），不是 HiveServer2（10000）。
     * 重构前走的是 `jdbc:hive2://`，每读一个分区就让 HiveServer2 起一个 MR/Tez 作业把数据
     * 序列化成结果集再一行行传回来；现在是拿到元数据后**直接读表目录下的文件**，
     * 和 Trino 读 Hive 是同一条路。
     *
     * `predicates` 是可选的读取端过滤：只下推**数据列**上的简单比较（AND 关系），
     * 用 ORC stripe / Parquet row group 的列统计（min/max）跳过不可能命中的分片，
     * 再在行级兜底过滤保证结果正确。分区级的裁剪仍走 `partition_filter`。
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
        /** 读取端谓词下推（AND 关系）。列必须是数据列或分区列，且为数值/字符串类型。 */
        val predicates: List<ConfigPredicate> = emptyList(),
        /**
         * 最多输出多少行（对标 Trino 的 `LIMIT`）。`<= 0` 表示不限制。
         *
         * 注意：并行 reader 下做不到"扫够 N 行就全局停 IO"（那是 Trino 单机协调器才能做的），
         * 这里下推为输出的 `Take`——结果最多 N 行、语义正确，但数据源仍会把整张表扫完。
         */
        val limit: Long = -1,
        /**
         * 采样下推（对标 Trino 的 `TABLESAMPLE BERNOULLI`）：每行以 [SampleConfig.fraction] 的概率被保留，
         * 直接在做行级过滤的 reader 里完成，不会把被丢掉的行发到下游（真正的下推，能减少下游数据量）。
         */
        val sample: SampleConfig? = null,
        /**
         * 聚合下推（对标 Trino 的 aggregation pushdown）：`count(*)` / `min(col)` / `max(col)`
         * 直接读 ORC/Parquet 文件尾的列统计，不扫行。与谓词/limit/sample 互斥（否则无法从统计推结果）。
         */
        val aggregates: List<ConfigAggregate> = emptyList(),
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
            val knownOps = setOf("=", "==", "eq", "!=", "<>", "neq", ">", "gt", ">=", "gte", "ge", "<", "lt", "<=", "lte", "le", "is null", "isnull", "is not null", "isnotnull")
            predicates.forEach { p ->
                require(p.column.isNotBlank()) { "predicates 里存在缺少 column 的谓词" }
                val op = p.op.trim().lowercase()
                require(op in knownOps) { "predicates 里列 [${p.column}] 的操作符 [$op] 不支持" }
                val isNullOp = op == "is null" || op == "isnull" || op == "is not null" || op == "isnotnull"
                if (!isNullOp) {
                    require(p.value.isNotBlank()) { "predicates 里列 [${p.column}] 的比较值不能为空" }
                }
            }
            require(limit > 0 || limit == -1L) { "limit 必须 > 0（或不限制时留空/传 -1）" }
            sample?.let { s ->
                require(s.fraction > 0.0 && s.fraction <= 1.0) { "sample.fraction 必须在 (0, 1] 之间" }
                SampleMethod.of(s.method) // 非法 method 显式报错，不静默退化
            }
            val knownAggTypes = setOf("count", "min", "max")
            aggregates.forEach { a ->
                require(a.type.trim().lowercase() in knownAggTypes) {
                    "aggregates 里类型 [${a.type}] 不支持，可选 count / min / max"
                }
                if (a.type.trim().lowercase() != "count") {
                    require(a.column.isNotBlank()) { "aggregates 里 [${a.type}] 必须指定 column" }
                }
            }
        }

    fun metastoreSpec(): HiveMetastoreSpec = HiveMetastoreSpec(metastoreUri, metastoreTimeoutMillis, hadoopConf)

    val qualifiedTable: String get() = "$database.$table"

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/** `ReadFromHive` 的采样下推配置（对标 Trino 的 `TABLESAMPLE`）。 */
data class SampleConfig(
    /** 每行被保留的概率，必须在 (0, 1]。 */
    val fraction: Double = 1.0,
    /** 采样方法：`bernoulli`（逐行随机，默认）或 `system`（按 stripe/row group 整块跳过，IO 更少）。 */
    val method: String = "bernoulli",
    /** 随机种子；不填则每次运行结果不同。 */
    val seed: Long? = null,
) : Serializable

/** `ReadFromHive` 的聚合下推配置（对标 Trino 的 `count` / `min` / `max`）。 */
data class ConfigAggregate(
    /** `count` / `min` / `max`；`min`/`max` 必须配 `column`。 */
    val type: String = "",
    /** `min`/`max` 作用的列名；`count` 忽略。 */
    val column: String = "",
) : Serializable

/** 采样方法，对齐 Trino 的 `TABLESAMPLE` 两种方式。 */
enum class SampleMethod {
    /** 逐行随机保留，对标 `TABLESAMPLE BERNOULLI`。 */
    BERNOULLI,

    /**
     * 按存储块（ORC stripe / Parquet row group）整块跳过，对标 `TABLESAMPLE SYSTEM`。
     * IO 更少（整段不读），但粒度是块；不可切分的格式退化成逐行。
     */
    SYSTEM,

    ;

    companion object {
        fun of(name: String): SampleMethod = entries.firstOrNull { it.name.equals(name.trim(), ignoreCase = true) }
            ?: throw IllegalArgumentException(
                "无法识别的 sample.method: $name，可选: ${entries.joinToString { it.name.lowercase() }}"
            )
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
