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
import org.apache.beam.sdk.coders.SerializableCoder
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
import java.util.concurrent.ThreadLocalRandom

/**
 * `ReadFromHive`: takes metadata from the metastore and reads the data files under the table directory directly.
 *
 * The whole chain mirrors Trino's Hive connector:
 *
 * ```
 * metastore(thrift)          -> table definition, partition list, and each partition's own storage format and directory
 *   -> Create(partition)     -> one element per partition
 *   -> ListFiles(DoFn)       -> the data files under the directory (hidden and empty files skipped)
 *   -> Read(Splittable DoFn) -> parallel read by byte range, claiming by stripe / row group / sync block / line
 * ```
 *
 * Compared with the pre-refactor version (`jdbc:hive2://` through HiveServer2) the difference is more than speed:
 *  - reading no longer needs HiveServer2 and no longer triggers MR/Tez jobs;
 *  - one file can be read by several workers instead of being "one indivisible processing unit per partition";
 *  - types come from the column definitions in the metastore instead of guessing from `DESCRIBE` and degrading to a single `value STRING` column;
 *  - partition values come from the directory name and are restored by the partition column type, instead of splicing `dt=2024-01-01/hr=01` into SQL predicates verbatim.
 *
 * @author wuya
 */
class HiveReadProvider : TypedTransformProvider<HiveReadConfig>(HiveReadConfig::class.java) {

    override fun identifier(): String = "ReadFromHive"

    override fun description(): String = "Takes metadata from the Hive metastore and reads the data files under the table directory directly, in parallel by byte range"

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
            "table does not exist: ${config.qualifiedTable}"
        }
        checkReadable(table)
        val format = HiveStorageFormat.of(table.storage.storageFormat)
        val aggSpecs = if (config.aggregates.isNotEmpty()) {
            // Aggregation pushdown is mutually exclusive with predicates/limit/sample: those all need rows first, while aggregation pushdown "does not even read rows"
            require(config.predicates.isEmpty()) { "aggregation pushdown cannot be combined with predicates" }
            require(config.limit <= 0) { "aggregation pushdown does not support limit" }
            require(config.sample == null) { "aggregation pushdown does not support sample" }
            require(format == HiveStorageFormat.ORC || format == HiveStorageFormat.PARQUET) {
                "aggregation pushdown supports only ORC / Parquet (other formats have no column statistics to push down), current format $format"
            }
            config.aggregates.map { buildAggSpec(it, table) }
        } else {
            emptyList()
        }
        if (aggSpecs.isNotEmpty()) {
            val partitions = resolvePartitions(metastore, table)
            val schema = aggregateSchema(aggSpecs)
            LOGGER.info(
                "ReadFromHive table[{}] aggregation pushdown aggSpecs={} partitionCount={}",
                table.qualifiedName,
                aggSpecs.map { "${it.type}:${it.column ?: "*"}" },
                partitions.size,
            )
            val partial = begin
                .apply(
                    "Partitions",
                    Create.of(partitions).withCoder(SerializableCoder.of(HivePartitionSpec::class.java)),
                )
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
            // When no seed is given explicitly, generate one per job; retries of the same job's workers still use the same seed.
            sampleSeed = config.sample?.let { it.seed ?: ThreadLocalRandom.current().nextLong() },
        )
        // Predicate columns must appear in the rows that are read, otherwise the row-level safety-net filter cannot decide and the pushdown silently does nothing.
        predicates.forEach { p ->
            require(spec.outputSchema.fieldNames.any { it.equals(p.column, ignoreCase = true) }) {
                "predicate column [${p.column}] is not among the read columns; list it explicitly in columns (or do not restrict columns)"
            }
        }
        val partitions = prunePartitions(resolvePartitions(metastore, table), predicates, table.partitionColumns)
        val schema = spec.outputSchema
        LOGGER.info(
            "ReadFromHive table[{}] format={} partitionCount={} predicateCount={} schema={}",
            table.qualifiedName,
            format,
            partitions.size,
            predicates.size,
            schema,
        )

        val read = begin
            .apply(
                "Partitions",
                Create.of(partitions).withCoder(SerializableCoder.of(HivePartitionSpec::class.java)),
            )
            .apply("ListFiles", ParDo.of(HiveListFilesFn(config.hadoopConf, config.recursiveDirectories)))
            .apply("Read", ParDo.of(HiveReadFn(spec, config.hadoopConf, config.splitBytes)))
            .setRowSchema(schema)
        // `LIMIT` is pushed down as Sample.any on the output: at most limit rows and semantically correct; with parallel readers there is
        // no guarantee of "stopping IO globally once enough rows are scanned" (Beam has no order-preserving head, and an SQL LIMIT without ORDER BY does not guarantee order anyway, so any satisfies the "<= N rows" semantics).
        if (config.limit > 0) read.apply("Limit pushdown", Sample.any(config.limit)) else read
    }

    /**
     * Parses one aggregate config into the [AggSpec] used for pushdown; a missing column or unsupported type fails explicitly here,
     */
    private fun buildAggSpec(cfg: ConfigAggregate, table: HiveTable): AggSpec {
        val type = AggType.of(cfg.type)
        if (type == AggType.COUNT) {
            return AggSpec(AggType.COUNT, null, FieldTypes.INT64, "count")
        }
        val column = requireNotNull(
            table.columns.firstOrNull { it.name.equals(cfg.column.trim(), ignoreCase = true) },
        ) { "aggregate column [${cfg.column}] does not exist on table ${table.qualifiedName}" }
        val fieldType = HiveTypes.parse(column.type)
        // MIN/MAX can take the lexicographic extreme of string/byte columns; SUM/AVG require a numeric column, otherwise the semantics do not hold
        val (allowed, hint) = when (type) {
            AggType.MIN, AggType.MAX ->
                setOf(
                    Schema.TypeName.BYTE, Schema.TypeName.INT16, Schema.TypeName.INT32, Schema.TypeName.INT64,
                    Schema.TypeName.FLOAT, Schema.TypeName.DOUBLE, Schema.TypeName.DECIMAL,
                    Schema.TypeName.STRING, Schema.TypeName.BYTES,
                ) to "only numeric, string and byte columns are supported"
            AggType.SUM, AggType.AVG ->
                setOf(
                    Schema.TypeName.BYTE, Schema.TypeName.INT16, Schema.TypeName.INT32, Schema.TypeName.INT64,
                    Schema.TypeName.FLOAT, Schema.TypeName.DOUBLE, Schema.TypeName.DECIMAL,
                ) to "must be a numeric column"
            AggType.COUNT -> emptySet<Schema.TypeName>() to ""
        }
        require(type == AggType.COUNT || fieldType.typeName in allowed) {
            "aggregation ${type.name} on column [${cfg.column}] of type ${fieldType.typeName} does not support pushdown yet, $hint"
        }
        val scale = if (fieldType.typeName == Schema.TypeName.DECIMAL) {
            HiveTypes.decimalPrecisionAndScale(column.type).second
        } else {
            0
        }
        return AggSpec(type, column.name, fieldType, "${type.name.lowercase()}_${column.name}", scale)
    }

    /**
     * A transactional (ACID) table's directory holds `delta_*` / `base_*` files plus row-level insert/delete/update markers,
     * so reading the files directly would also return rows that were already deleted. Such tables must be rejected outright
     */
    private fun checkReadable(table: HiveTable) {
        require(table.tableType != HiveTable.VIRTUAL_VIEW) {
            "${table.qualifiedName} is a view, ReadFromHive can only read tables"
        }
        require(table.parameters["transactional"]?.toBoolean() != true) {
            "${table.qualifiedName} is a transactional (ACID) table, not supported yet: its directory holds delta/base incremental files, " +
                "so reading the files directly would return rows that were already deleted"
        }
        // If the format cannot be determined, this throws right here — much earlier than reading a pile of garbage
        HiveStorageFormat.of(table.storage.storageFormat)
    }

    /**
     * Decides which partitions to read this time, carrying each partition's own storage description along.
     *
     * The partition-level storage description cannot be skipped: `ALTER TABLE ... PARTITION (...) SET FILEFORMAT` is legal, and
     * different partitions of one table using different formats is common in production.
     */
    private fun resolvePartitions(metastore: HiveMetastore, table: HiveTable): List<HivePartitionSpec> {
        if (!table.partitioned) {
            require(config.partitions.isEmpty() && config.partitionFilter.isBlank()) {
                "${table.qualifiedName} is not a partitioned table, so partitions / partition_filter must not be configured"
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
            "${table.qualifiedName} has no matching partition" +
                (if (config.partitionFilter.isNotBlank()) " (filter: ${config.partitionFilter})" else "")
        }
        val partitions = metastore.getPartitionsByNames(config.database, config.table, names)
        val missing = names.filterNot { it in partitions }
        require(missing.isEmpty()) { "these partitions do not exist on ${table.qualifiedName}: $missing" }
        return names.map { HivePartitionSpec.of(it, partitions.getValue(it)) }
    }

    /**
     * Partition pruning: pick the predicates on **partition columns** out of `predicates` and drop the partitions that cannot
     * contain a matching row at graph construction time (a table that is a single partition naturally has nothing to prune).
     *
     * Orthogonal to `partition_filter` — when both exist their intersection is taken; predicates on partition columns that survive
     * still take part in row-level filtering, which does not affect correctness (a partition column is constant within a partition,
     * so it is always TRUE). Three-valued logic is conservative: when unsure, keep the partition and never lose data. This way writing
     * `predicates: [{column: dt, op: "=", value: "2024-01-02"}]` automatically skips irrelevant partitions, without a separate
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
                "ReadFromHive partition pruning: {} partitions pruned down to {} (a predicate hit a partition column)",
                partitions.size,
                kept.size,
            )
        }
        return kept
    }

    /** Column names inside a partition name are normalized to lowercase, matching what the metastore stores. */
    private fun normalizePartitionName(name: String): String {
        val columns = PartitionNames.toPartitionColumnNames(name)
        val values = PartitionNames.toPartitionValues(name)
        return PartitionNames.makePartName(columns, values)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }

    /**
     * Parses the raw predicates from the config into the [HivePredicate] used for pushdown; a missing column or unsupported type
     * fails explicitly here instead of degrading silently (following the "a config option either takes effect or is not accepted at all" rule).
     */
    private fun parsePredicates(configs: List<ConfigPredicate>, table: HiveTable): List<HivePredicate> {
        if (configs.isEmpty()) return emptyList()
        val byName = table.columns.associateBy { it.name.lowercase() }
        return configs.map { cfg ->
            val column = requireNotNull(byName[cfg.column.lowercase()]) {
                "predicate column [${cfg.column}] does not exist on table ${table.qualifiedName}"
            }
            parsePredicate(cfg, HiveTypes.parse(column.type))
        }
    }
}

private val LOGGER = LoggerFactory.getLogger(HiveReadProvider::class.java)

/**
 * Merge side of aggregation pushdown: collects all "partial aggregates" (one row per file) through a side input and merges them
 * into the final result in one go. This data is tiny (one row per file), so there is no distributed Combine; it is done inside a
 * single DoFn, which also avoids the coder inference problems of the KV accumulators inside [org.apache.beam.sdk.transforms.Combine].
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
