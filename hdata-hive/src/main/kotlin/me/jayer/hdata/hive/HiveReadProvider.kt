package me.jayer.hdata.hive

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.hive.format.ConfigPredicate
import me.jayer.hdata.hive.format.HivePredicate
import me.jayer.hdata.hive.format.PredicateEvaluator
import me.jayer.hdata.hive.format.HiveReadSpec
import me.jayer.hdata.hive.format.HiveStorageFormat
import me.jayer.hdata.hive.format.parsePredicate
import me.jayer.hdata.hive.metastore.HiveMetastore
import me.jayer.hdata.hive.metastore.HiveMetastores
import me.jayer.hdata.hive.metastore.HiveTable
import me.jayer.hdata.hive.metastore.HiveColumn
import me.jayer.hdata.hive.metastore.PartitionNames
import me.jayer.hdata.hive.split.HivePartitionSpec
import me.jayer.hdata.hive.transform.HiveListFilesFn
import me.jayer.hdata.hive.transform.HiveReadFn
import me.jayer.hdata.hive.type.HiveTypes
import me.jayer.hdata.hive.SampleMethod
import me.jayer.hdata.core.type.FieldTypes
import me.jayer.hdata.hive.format.AggSpec
import me.jayer.hdata.hive.format.AggType
import me.jayer.hdata.hive.format.HiveAggregateFn
import me.jayer.hdata.hive.format.aggregateSchema
import me.jayer.hdata.hive.format.aggregateAccumSchema
import me.jayer.hdata.hive.format.mergeAggregatePartials
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.Sample
import org.apache.beam.sdk.transforms.View
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.PCollectionRowTuple
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
        val format = HiveStorageFormat.of(table.storage.storageFormat)
        val aggSpecs = if (config.aggregates.isNotEmpty()) {
            // 聚合下推与谓词/limit/sample 互斥：这些都要先读出行才能算，而聚合下推是"连行都不读"。
            require(config.predicates.isEmpty()) { "聚合下推不支持与 predicates 同时使用" }
            require(config.limit <= 0) { "聚合下推不支持 limit" }
            require(config.sample == null) { "聚合下推不支持 sample" }
            require(format == HiveStorageFormat.ORC || format == HiveStorageFormat.PARQUET) {
                "聚合下推仅支持 ORC / Parquet（其它格式没有列统计，无法下推），当前格式 $format"
            }
            config.aggregates.map { buildAggSpec(it, table) }
        } else {
            emptyList()
        }
        if (aggSpecs.isNotEmpty()) {
            val partitions = resolvePartitions(metastore, table)
            val schema = aggregateSchema(aggSpecs)
            LOGGER.info(
                "ReadFromHive 表[{}] 聚合下推 aggSpecs={} 分区数={}",
                table.qualifiedName,
                aggSpecs.map { "${it.type}:${it.column ?: "*"}" },
                partitions.size,
            )
            val partial = begin
                .apply("Partitions", Create.of(partitions))
                .apply("ListFiles", ParDo.of(HiveListFilesFn(config.hadoopConf, config.recursiveDirectories)))
                .apply("Aggregate", ParDo.of(HiveAggregateFn(aggSpecs, config.hadoopConf)))
                .setRowSchema(aggregateAccumSchema(aggSpecs))
            val view = partial.apply(View.asList())
            return@withMetastore begin.pipeline
                .apply("MergeDriver", Create.of("merge"))
                .apply("Merge aggregates", ParDo.of(HiveMergeFn(aggSpecs, view)).withSideInputs(view))
                .setRowSchema(schema)
        }

        val baseSpec = HiveReadSpec.of(table, config.columns)
        val predicates = parsePredicates(config.predicates, table)
        val spec = baseSpec.copy(
            predicates = predicates,
            limit = config.limit,
            sampleFraction = config.sample?.fraction ?: 1.0,
            sampleMethod = config.sample?.let { SampleMethod.of(it.method) } ?: SampleMethod.BERNOULLI,
            sampleSeed = config.sample?.seed,
        )
        // 谓词列必须出现在读取出的行里，行级兜底过滤才能正确判定；否则下推等于静默失效。
        predicates.forEach { p ->
            require(spec.outputSchema.fieldNames.any { it.equals(p.column, ignoreCase = true) }) {
                "谓词列 [${p.column}] 不在读取的列中，请在 columns 里显式列出它（或不要限制 columns）"
            }
        }
        val partitions = prunePartitions(resolvePartitions(metastore, table), predicates, table.partitionColumns)
        val schema = spec.outputSchema
        LOGGER.info(
            "ReadFromHive 表[{}] 格式={} 分区数={} 谓词数={} schema={}",
            table.qualifiedName,
            format,
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
     * 把一个聚合配置解析成下推用的 [AggSpec]；列不存在 / 类型不支持都在这里显式报错，不静默退化。
     */
    private fun buildAggSpec(cfg: ConfigAggregate, table: HiveTable): AggSpec {
        val type = AggType.of(cfg.type)
        if (type == AggType.COUNT) {
            return AggSpec(AggType.COUNT, null, FieldTypes.INT64, "count")
        }
        val column = requireNotNull(
            table.columns.firstOrNull { it.name.equals(cfg.column.trim(), ignoreCase = true) },
        ) { "聚合列 [${cfg.column}] 在表 ${table.qualifiedName} 上不存在" }
        val fieldType = HiveTypes.parse(column.type)
        // MIN/MAX 能对字符串/字节列取字典序极值；SUM/AVG 必须有数值列，否则语义不成立。
        val (allowed, hint) = when (type) {
            AggType.MIN, AggType.MAX ->
                setOf(
                    Schema.TypeName.BYTE, Schema.TypeName.INT16, Schema.TypeName.INT32, Schema.TypeName.INT64,
                    Schema.TypeName.FLOAT, Schema.TypeName.DOUBLE, Schema.TypeName.DECIMAL,
                    Schema.TypeName.STRING, Schema.TypeName.BYTES,
                ) to "只支持数值、字符串与字节列"
            AggType.SUM, AggType.AVG ->
                setOf(
                    Schema.TypeName.BYTE, Schema.TypeName.INT16, Schema.TypeName.INT32, Schema.TypeName.INT64,
                    Schema.TypeName.FLOAT, Schema.TypeName.DOUBLE, Schema.TypeName.DECIMAL,
                ) to "必须是数值列"
            AggType.COUNT -> emptySet<Schema.TypeName>() to ""
        }
        require(type == AggType.COUNT || fieldType.typeName in allowed) {
            "聚合 ${type.name} 的列 [${cfg.column}] 类型 ${fieldType.typeName} 暂不支持下推，$hint"
        }
        val scale = if (fieldType.typeName == Schema.TypeName.DECIMAL) {
            HiveTypes.decimalPrecisionAndScale(column.type).second
        } else {
            0
        }
        return AggSpec(type, column.name, fieldType, "${type.name.lowercase()}_${column.name}", scale)
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

    /**
     * 分区裁剪（partition pruning）：从 `predicates` 里挑出**分区列**上的谓词，直接在构图阶段
     * 过滤掉不可能含命中行的分区（整张表就是一个分区时自然没有可裁剪的）。
     *
     * 与 `partition_filter` 正交——两者都存在时取交集；被保留下来的分区列谓词仍会继续参与行级过滤，
     * 不影响正确性（分区内分区列值是常量，只会恒为 TRUE）。三值逻辑保守：判不准就保留，绝不丢数据。
     * 这样用户写 `predicates: [{column: dt, op: "=", value: "2024-01-02"}]` 就能自动跳过无关分区，
     * 不必再单独配 `partition_filter`。
     */
    private fun prunePartitions(
        partitions: List<HivePartitionSpec>,
        predicates: List<HivePredicate>,
        partitionColumns: List<HiveColumn>,
    ): List<HivePartitionSpec> {
        if (partitionColumns.isEmpty() || predicates.isEmpty()) return partitions
        val byName = partitionColumns.associateBy { it.name.lowercase() }
        val pcPredicates = predicates.filter { it.column.lowercase() in byName }
        if (pcPredicates.isEmpty()) return partitions
        val kept = partitions.filter { spec ->
            pcPredicates.all { p ->
                val idx = partitionColumns.indexOfFirst { c -> c.name.lowercase() == p.column.lowercase() }
                val raw = spec.values.getOrNull(idx)
                PredicateEvaluator.matchesConstant(p, raw)
            }
        }
        if (kept.size != partitions.size) {
            LOGGER.info(
                "ReadFromHive 分区裁剪：{} 个分区裁剪为 {} 个（谓词命中分区列）",
                partitions.size,
                kept.size,
            )
        }
        return kept
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

/**
 * 聚合下推的归并端：借助 side input 把所有"部分聚合"（每个文件一行）收齐后一次性归并成最终结果。
 * 这部分数据量极小（每个文件一行），所以不做分布式 Combine，直接在一个 DoFn 内完成，
 * 同时避开 [org.apache.beam.sdk.transforms.Combine] 内部 KV 累加器的 coder 推断问题。
 */
class HiveMergeFn(
    private val aggregates: List<AggSpec>,
    private val view: PCollectionView<List<Row>>,
) : DoFn<String, Row>() {

    @ProcessElement
    fun processElement(@Element dummy: String, out: OutputReceiver<Row>, c: ProcessContext) {
        val partials: List<Row> = c.sideInput(view)
        out.output(mergeAggregatePartials(aggregates, partials))
    }
}
