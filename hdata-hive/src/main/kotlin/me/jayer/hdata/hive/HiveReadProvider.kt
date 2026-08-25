package me.jayer.hdata.hive

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.hive.format.ConfigPredicate
import me.jayer.hdata.hive.format.HivePredicate
import me.jayer.hdata.hive.format.HiveReadSpec
import me.jayer.hdata.hive.format.HiveStorageFormat
import me.jayer.hdata.hive.format.parsePredicate
import me.jayer.hdata.hive.metastore.HiveMetastore
import me.jayer.hdata.hive.metastore.HiveMetastores
import me.jayer.hdata.hive.metastore.HiveTable
import me.jayer.hdata.hive.metastore.PartitionNames
import me.jayer.hdata.hive.split.HivePartitionSpec
import me.jayer.hdata.hive.transform.HiveListFilesFn
import me.jayer.hdata.hive.transform.HiveReadFn
import me.jayer.hdata.hive.type.HiveTypes
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.Sample
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory

/**
 * `ReadFromHive`：从 metastore 拿元数据，直接读表目录下的数据文件。
 *
 * 整条链路对齐 Trino 的 Hive 连接器：
 *
 * ```
 * metastore(thrift)          -> 表定义、分区列表、每个分区自己的存储格式与目录
 *   -> Create(分区)          -> 每个分区一个元素
 *   -> ListFiles(DoFn)       -> 目录下的数据文件（跳过隐藏文件与空文件）
 *   -> Read(Splittable DoFn) -> 按字节区间并行读，按 stripe / row group / 同步块 / 行认领
 * ```
 *
 * 与重构前（`jdbc:hive2://` 走 HiveServer2）的区别不只是快慢：
 *  - 读取不再需要 HiveServer2，也不再触发 MR/Tez 作业；
 *  - 单个文件可以被多个 worker 分着读，而不是"一个分区一个不可再分的处理单元"；
 *  - 类型来自 metastore 上的列定义，不再靠 `DESCRIBE` 猜、猜不出来就退化成单列 `value STRING`；
 *  - 分区值来自目录名并按分区列的类型还原，不再把 `dt=2024-01-01/hr=01` 原样拼进 SQL 谓词。
 *
 * @author wuya
 */
class HiveReadProvider : TypedTransformProvider<HiveReadConfig>(HiveReadConfig::class.java) {

    override fun identifier(): String = "ReadFromHive"

    override fun description(): String = "从 Hive metastore 取元数据后直接读表目录下的数据文件，按字节区间并行"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(
        config: HiveReadConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return HiveSource(config)
    }
}

private class HiveSource(private val config: HiveReadConfig) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> =
        HiveMetastores.withMetastore(config.metastoreSpec()) { metastore ->
            val table = requireNotNull(metastore.getTable(config.database, config.table)) {
                "表不存在: ${config.qualifiedTable}"
            }
            checkReadable(table)
            val baseSpec = HiveReadSpec.of(table, config.columns)
            val predicates = parsePredicates(config.predicates, table)
            val spec = baseSpec.copy(
                predicates = predicates,
                limit = config.limit,
                sampleFraction = config.sample?.fraction ?: 1.0,
                sampleSeed = config.sample?.seed,
            )
            // 谓词列必须出现在读取出的行里，行级兜底过滤才能正确判定；否则下推等于静默失效。
            predicates.forEach { p ->
                require(spec.outputSchema.fieldNames.any { it.equals(p.column, ignoreCase = true) }) {
                    "谓词列 [${p.column}] 不在读取的列中，请在 columns 里显式列出它（或不要限制 columns）"
                }
            }
            val partitions = resolvePartitions(metastore, table)
            val schema = spec.outputSchema
            LOGGER.info(
                "ReadFromHive 表[{}] 格式={} 分区数={} 谓词数={} schema={}",
                table.qualifiedName,
                HiveStorageFormat.of(table.storage.storageFormat),
                partitions.size,
                predicates.size,
                schema,
            )

            val read = begin
                .apply("Partitions", Create.of(partitions))
                .apply("ListFiles", ParDo.of(HiveListFilesFn(config.hadoopConf, config.recursiveDirectories)))
                .apply("Read", ParDo.of(HiveReadFn(spec, config.hadoopConf, config.splitBytes)))
                .setRowSchema(schema)
            // `LIMIT` 下推为输出的 Sample.any：结果最多 limit 行、语义正确；并行 reader 下不保证"扫够就全局停 IO"
            // （Beam 没有保序的 head，SQL 的 LIMIT 不带 ORDER BY 时顺序本就不保证，any 满足"≤N 行"的语义）。
            if (config.limit > 0) read.apply("Limit pushdown", Sample.any(config.limit)) else read
        }

    /**
     * 事务表（ACID）的目录里是 `delta_*` / `base_*` 加上行级的增删改标记，
     * 直接按文件读会把已经删掉的行也读出来。这种表必须明确拒绝，不能装作能读。
     */
    private fun checkReadable(table: HiveTable) {
        require(table.tableType != HiveTable.VIRTUAL_VIEW) {
            "${table.qualifiedName} 是视图，ReadFromHive 只能读表"
        }
        require(table.parameters["transactional"]?.toBoolean() != true) {
            "${table.qualifiedName} 是事务表(ACID)，暂不支持：它的目录里是 delta/base 增量文件，" +
                "直接按文件读会读出已经删除的行"
        }
        // 格式判不出来的话这里就会抛，比读出一堆乱码早得多
        HiveStorageFormat.of(table.storage.storageFormat)
    }

    /**
     * 决定这次要读哪些分区，并把每个分区自己的存储描述带上。
     *
     * 分区级的存储描述不能省：`ALTER TABLE ... PARTITION (...) SET FILEFORMAT` 是合法的，
     * 一张表里不同分区用不同格式在生产里很常见。
     */
    private fun resolvePartitions(metastore: HiveMetastore, table: HiveTable): List<HivePartitionSpec> {
        if (!table.partitioned) {
            require(config.partitions.isEmpty() && config.partitionFilter.isBlank()) {
                "${table.qualifiedName} 不是分区表，不能配 partitions / partition_filter"
            }
            return listOf(HivePartitionSpec.unpartitioned(table.storage))
        }
        val names = when {
            config.partitions.isNotEmpty() -> config.partitions.map(::normalizePartitionName)
            config.partitionFilter.isNotBlank() ->
                metastore.getPartitionNamesByFilter(config.database, config.table, config.partitionFilter)

            else -> metastore.getPartitionNames(config.database, config.table)
        }
        require(names.isNotEmpty()) {
            "${table.qualifiedName} 没有匹配到任何分区" +
                (if (config.partitionFilter.isNotBlank()) "（过滤条件: ${config.partitionFilter}）" else "")
        }
        val partitions = metastore.getPartitionsByNames(config.database, config.table, names)
        val missing = names.filterNot { it in partitions }
        require(missing.isEmpty()) { "${table.qualifiedName} 上不存在这些分区: $missing" }
        return names.map { HivePartitionSpec.of(it, partitions.getValue(it)) }
    }

    /** 分区名里的列名统一成小写，与 metastore 存的一致。 */
    private fun normalizePartitionName(name: String): String {
        val columns = PartitionNames.toPartitionColumnNames(name)
        val values = PartitionNames.toPartitionValues(name)
        return PartitionNames.makePartName(columns, values)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }

    /**
     * 把配置里的原始谓词解析成下推用的 [HivePredicate]；列不存在或类型不支持都在这里显式报错，
     * 不静默退化（对齐"配置要么生效要么就别收"的原则）。
     */
    private fun parsePredicates(configs: List<ConfigPredicate>, table: HiveTable): List<HivePredicate> {
        if (configs.isEmpty()) return emptyList()
        val byName = table.columns.associateBy { it.name.lowercase() }
        return configs.map { cfg ->
            val column = requireNotNull(byName[cfg.column.lowercase()]) {
                "谓词列 [${cfg.column}] 在表 ${table.qualifiedName} 上不存在"
            }
            parsePredicate(cfg, HiveTypes.parse(column.type))
        }
    }
}

private val LOGGER = LoggerFactory.getLogger(HiveReadProvider::class.java)
