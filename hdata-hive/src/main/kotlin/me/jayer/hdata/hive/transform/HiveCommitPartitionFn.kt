package me.jayer.hdata.hive.transform

import me.jayer.hdata.hive.HiveWriteMode
import me.jayer.hdata.hive.metastore.HiveMetastore
import me.jayer.hdata.hive.metastore.HiveMetastoreSpec
import me.jayer.hdata.hive.metastore.HiveMetastores
import me.jayer.hdata.hive.metastore.HivePartition
import me.jayer.hdata.hive.metastore.PartitionNames
import me.jayer.hdata.hive.metastore.Storage
import me.jayer.hdata.hive.split.HiveFileSystems
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.KV
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

/**
 * 落盘之后的提交步骤，对应 Trino 的 `HiveMetadata.finishInsert`：
 * 覆盖模式下清掉分区里的旧文件，然后把新分区注册进 metastore。
 *
 * 输入是 `FileIO` 的 `getPerDestinationOutputFilenames()` 按分区聚好的结果，
 * 也就是"这个分区这次写出了哪些文件"。因此：
 *  - 没有数据落到的分区**根本不会出现在输入里**，覆盖模式不会误伤它们
 *    （与 Hive 动态分区覆盖的语义一致）；
 *  - 删除时按文件名把本次写出的文件排除在外，不依赖时间戳之类不可靠的判据。
 *
 * 这一步是**幂等**的：bundle 重试时旧文件已经删掉了，本次写出的文件仍在集合里不会被删。
 *
 * 注意覆盖不是原子的：新文件已经改名到位、旧文件还没删完的那一小段时间里，
 * 读的人会同时看到两份数据。没有 ACID 的 Hive 表本来就是这个样子（Hive 自己也一样），
 * 要强一致只能上事务表。
 *
 * @author wuya
 */
class HiveCommitPartitionFn(
    private val metastoreSpec: HiveMetastoreSpec,
    private val databaseName: String,
    private val tableName: String,
    private val partitionColumnNames: List<String>,
    private val tableStorage: Storage,
    private val writeMode: HiveWriteMode,
    private val createPartitions: Boolean,
    private val hadoopConf: Map<String, String>,
) : DoFn<KV<String, @JvmSuppressWildcards Iterable<String>>, String>() {

    // @JvmSuppressWildcards 是必须的：Kotlin 的 Iterable<out E> 编译成 Java 签名会变成
    // Iterable<? extends String>，而 GroupByKey 产出的是 Iterable<String>，
    // Beam 用反射比对 DoFn 的输入类型，对不上就直接报 "Type of @Element must match the DoFn type"。

    @Transient
    private var metastore: HiveMetastore? = null

    @Setup
    fun setup() {
        metastore = HiveMetastores.create(metastoreSpec)
    }

    @Teardown
    fun tearDown() {
        metastore?.close()
        metastore = null
    }

    @ProcessElement
    fun processElement(
        @Element element: KV<String, @JvmSuppressWildcards Iterable<String>>,
        receiver: OutputReceiver<String>,
    ) {
        val partitionName = element.key
        // Beam 报回来的是完整路径，但它已经过 HivePaths.forBeamIO 去掉了 scheme，
        // 而 Hadoop 列出来的路径是带 scheme 的，两者只能按文件名比
        val written = element.value.map { Path(it).name }.toSet()

        if (writeMode == HiveWriteMode.OVERWRITE) {
            removeStaleFiles(partitionName, written)
        }
        if (partitionName.isNotEmpty() && createPartitions) {
            addPartition(partitionName)
        }
        receiver.output(partitionName)
    }

    /** 删掉这个分区目录下**不是本次写出**的数据文件。 */
    private fun removeStaleFiles(partitionName: String, written: Set<String>) {
        val configuration = HiveFileSystems.configurationOf(hadoopConf)
        val location = partitionLocation(partitionName)
        // 不递归：分区目录下再有子目录只可能是 ACID 的 delta/base，那种表在读写两端都已经明确拒绝了
        val stale = HiveFileSystems.listFiles(configuration, location, recursive = false)
            .filter { it.path.name !in written }
        if (stale.isEmpty()) {
            return
        }
        val fs = Path(location).getFileSystem(configuration)
        stale.forEach { status ->
            if (fs.delete(status.path, false)) {
                FILES_DELETED.inc()
            } else {
                LOGGER.warn("覆盖写入时删不掉旧文件: {}", status.path)
            }
        }
        LOGGER.info(
            "覆盖写入: 分区[{}] 删掉 {} 个旧文件，保留本次写出的 {} 个",
            partitionName.ifEmpty { "<非分区表>" },
            stale.size,
            written.size,
        )
    }

    private fun addPartition(partitionName: String) {
        val values = PartitionNames.toPartitionValues(partitionName)
        require(values.size == partitionColumnNames.size) {
            "分区名[$partitionName] 的值个数与分区列 $partitionColumnNames 对不上"
        }
        val partition = HivePartition(
            values = values,
            storage = tableStorage.copy(location = partitionLocation(partitionName)),
        )
        val added = checkNotNull(metastore) { "metastore 客户端未初始化" }
            .addPartitions(databaseName, tableName, mapOf(partitionName to partition))
        if (added.isNotEmpty()) {
            PARTITIONS_ADDED.inc()
            LOGGER.info("已注册新分区 {}.{} {}", databaseName, tableName, partitionName)
        }
    }

    private fun partitionLocation(partitionName: String): String {
        val base = tableStorage.location.trimEnd('/')
        return if (partitionName.isEmpty()) base else "$base/$partitionName"
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(HiveCommitPartitionFn::class.java)
        private val PARTITIONS_ADDED = Metrics.counter(HiveCommitPartitionFn::class.java, "partitions_added")
        private val FILES_DELETED = Metrics.counter(HiveCommitPartitionFn::class.java, "files_deleted")
    }
}
