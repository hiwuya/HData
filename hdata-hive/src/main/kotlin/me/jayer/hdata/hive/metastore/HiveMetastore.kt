package me.jayer.hdata.hive.metastore

import java.io.Serializable

/**
 * 元数据访问的抽象，形态参照 Trino 的 `HiveMetastore`。
 *
 * 分成接口是有实际收益的，不是为了分层而分层：
 *  - 生产上是 [ThriftHiveMetastore]，直接说 metastore 的 thrift 协议，不经过 HiveServer2；
 *  - 测试里是 [InMemoryHiveMetastore]，`mvn test` 因此不需要任何外部服务；
 *  - 将来要接 Glue / DLF / 文件型 metastore，加一个实现即可，读写两端一行都不用动。
 *
 * 实现必须是**每个 worker 各建一个**（`@Setup` 建、`@Teardown` 关），
 * 不要试图把实现本身塞进 DoFn 序列化下发——thrift 的 socket 不可序列化。
 * 可序列化的是 [HiveMetastoreSpec]。
 *
 * @author wuya
 */
interface HiveMetastore : AutoCloseable {

    /** 表不存在时返回 null，而不是抛异常。 */
    fun getTable(databaseName: String, tableName: String): HiveTable?

    /** 全部分区名，形如 `dt=2024-01-01/hr=01`。非分区表返回空列表。 */
    fun getPartitionNames(databaseName: String, tableName: String): List<String>

    /**
     * 按 metastore 的分区过滤表达式取分区名，例如 `dt = "2024-01-01"`。
     *
     * 交给 metastore 而不是拉全量分区名再在客户端过滤：分区数上万的表，
     * 一次 `get_partition_names` 的返回体就是几 MB。
     */
    fun getPartitionNamesByFilter(databaseName: String, tableName: String, filter: String): List<String>

    /** 批量取分区详情，返回值按分区名索引；不存在的分区不会出现在结果里。 */
    fun getPartitionsByNames(
        databaseName: String,
        tableName: String,
        partitionNames: List<String>,
    ): Map<String, HivePartition>

    /**
     * 注册分区。已存在的分区**跳过而不是报错**——写入端可能重跑，也可能同时往同一个分区里追加数据。
     *
     * @return 真正新建的分区名
     */
    fun addPartitions(databaseName: String, tableName: String, partitions: Map<String, HivePartition>): List<String>

    /** 建表。主要给测试和"目标表不存在就建"的场景用。 */
    fun createTable(table: HiveTable)

    override fun close() {}
}

/**
 * 可序列化的 metastore 连接声明，随 DoFn 下发到 worker，由 [HiveMetastores.create] 还原成客户端。
 *
 * @param uri `thrift://host:9083`（可逗号分隔多个做 HA），或 `memory://<名字>`（进程内，测试用）
 * @param timeoutMillis socket 读写超时
 * @param configuration 透传给 Hadoop `Configuration` 的键值对，例如 `fs.defaultFS`
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
 * 按 [HiveMetastoreSpec.uri] 的 scheme 选实现。
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
                "无法识别的 metastore_uri: ${spec.uri}，应为 thrift://host:9083 或 memory://<名字>"
            )
        }
    }

    /** 建一个临时客户端跑一段逻辑，用完即关。构图阶段取元数据用这个。 */
    fun <T> withMetastore(spec: HiveMetastoreSpec, block: (HiveMetastore) -> T): T =
        create(spec).use(block)
}
