package me.jayer.hdata.hive

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.core.spi.DeliveryCapabilities
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.OrderingScope
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.hive.format.HiveFileSinks
import me.jayer.hdata.hive.format.HiveStorageFormat
import me.jayer.hdata.hive.metastore.HiveMetastores
import me.jayer.hdata.hive.metastore.HiveTable
import me.jayer.hdata.hive.split.HivePaths
import me.jayer.hdata.hive.transform.HiveCommitPartitionFn
import me.jayer.hdata.hive.transform.HiveRowToRecordFn
import me.jayer.hdata.hive.type.HiveTypes
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.schemas.SchemaCoder
import org.apache.beam.sdk.transforms.Contextful
import org.apache.beam.sdk.transforms.GroupByKey
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.PCollectionTuple
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.TupleTag
import org.apache.beam.sdk.values.TupleTagList
import org.slf4j.LoggerFactory

/**
 * `WriteToHive`: writes files into the table directory following the table's storage format, then registers new partitions in the metastore.
 *
 * Shaped after Trino's write path (`HivePageSink` writes the files, `finishInsert` commits the metadata):
 *
 * ```
 * input Row
 *   -> ToRecord(DoFn)          -> KV<partition name, row holding data columns only>, failed rows go to the dead letter
 *   -> FileIO.writeDynamic()   -> writes into one directory per partition name; each shard writes a temp file and all are renamed atomically on success
 *   -> GroupByKey + Commit     -> overwrite mode clears old files, new partitions are registered in the metastore
 * ```
 *
 * Partitioning is **dynamic**: each row decides which partition it lands in from its own partition column values, and one job writes
 * any number of partitions, matching Hive's dynamic partition insert. `write_mode` decides what happens to existing data (see [HiveWriteMode]).
 *
 * We use `FileIO.writeDynamic()` instead of opening files inside a DoFn because the latter truncates already written results when the
 * job retries — Beam does not guarantee `@Setup`/`@Teardown` run only once per worker.
 *
 * Before the refactor this went through a JDBC `INSERT INTO`: one network round trip per batch, and HiveServer2 also started a job for
 * every INSERT, with no control over the file format written to disk either.
 *
 * @author wuya
 */
class HiveWriteProvider : TypedTransformProvider<HiveWriteConfig>(HiveWriteConfig::class.java) {

    override fun identifier(): String = "WriteToHive"

    override fun description(): String = "Writes files under the table directory following the table's storage format and registers new partitions in the metastore"

    override fun outputCollectionNames(): List<String> = listOf(Tags.ERROR_OUTPUT)

    // EXPERIMENTAL: see HiveReadProvider.supportTier.
    override fun supportTier(): ConnectorSupportTier = ConnectorSupportTier.EXPERIMENTAL

    override fun deliveryCapabilities(config: TransformConfig): DeliveryCapabilities {
        val overwrite = config.bind(HiveWriteConfig::class.java).mode() == HiveWriteMode.OVERWRITE
        return DeliveryCapabilities(
            deliveryMode = DeliveryMode.AT_LEAST_ONCE,
            replayBehavior = ReplayBehavior.NOT_APPLICABLE,
            ordering = OrderingScope.NONE,
            requiresIdempotencyKey = !overwrite,
            notes = if (overwrite) {
                "write_mode=overwrite clears the partitions written this run before committing, so rerunning " +
                    "the whole job produces the same result. Files commit atomically (FileIO.writeDynamic), " +
                    "but metastore partition registration is a separate step after that: a crash between the " +
                    "two can leave written files not yet visible as partitions."
            } else {
                "write_mode=append (INSERT INTO semantics): rerunning the whole job adds another full copy of " +
                    "the data on top of what a prior run already committed. Files commit atomically " +
                    "(FileIO.writeDynamic), but metastore partition registration is a separate step after " +
                    "that: a crash between the two can leave written files not yet visible as partitions."
            },
        )
    }

    override fun create(
        config: HiveWriteConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return HiveSink(config, context.errorHandling != null, context.transformName)
    }
}

private class HiveSink(
    private val config: HiveWriteConfig,
    private val deadLetter: Boolean,
    private val transformName: String,
) : RowSink() {

    override fun write(input: PCollection<Row>): PCollection<Row>? =
        HiveMetastores.withMetastore(config.metastoreSpec()) { metastore ->
            val table = requireNotNull(metastore.getTable(config.database, config.table)) {
                "table does not exist: ${config.qualifiedTable}. WriteToHive does not create tables, please create it in Hive first"
            }
            checkWritable(table)
            val format = HiveStorageFormat.of(table.storage.storageFormat)
            // The data files contain only data columns; partition column values are encoded in the directory name
            val fileSchema = HiveTypes.schemaOf(table.dataColumns)
            val errorSchema = ErrorSchemas.of(input.schema)
            LOGGER.info(
                "WriteToHive table[{}] format={} location={} partitionColumns={}",
                table.qualifiedName,
                format,
                table.storage.location,
                table.partitionColumns.map { it.name },
            )

            val errorTag = TupleTag<Row>()
            val mainTag = object : TupleTag<KV<String, Row>>() {}
            val outputs: PCollectionTuple = input.apply(
                "ToRecord",
                ParDo.of(
                    HiveRowToRecordFn(
                        fileSchema,
                        table.partitionColumns,
                        errorSchema,
                        deadLetter,
                        transformName,
                        errorTag,
                    )
                ).withOutputTags(mainTag, TupleTagList.of(errorTag)),
            )

            val records = outputs.get(mainTag)
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SchemaCoder.of(fileSchema)))
            writeFiles(records, table, format, fileSchema)

            val errors = outputs.get(errorTag).setRowSchema(errorSchema)
            if (deadLetter) errors else null
        }

    private fun writeFiles(
        records: PCollection<KV<String, Row>>,
        table: HiveTable,
        format: HiveStorageFormat,
        fileSchema: Schema,
    ) {
        val sink = HiveFileSinks.of(
            format = format,
            columns = table.dataColumns,
            schema = fileSchema,
            serdeParameters = table.storage.serdeParameters,
            configuration = config.hadoopConf,
        )
        val extension = format.fileExtension
        // Beam's defaultNaming names files only by "prefix-shardNumber-of-total", so running the same table twice produces
        // **identical** file names and the second run overwrites the first run's result — neither an append nor an overwrite,
        // just silently lost data. So every job carries a unique token, and an append write is really an append.
        val prefix = "${config.filePrefix}-${runToken()}"
        var write = FileIO.writeDynamic<String, KV<String, Row>>()
            .by { it.key }
            .withDestinationCoder(StringUtf8Coder.of())
            .via(Contextful.fn<KV<String, Row>, Row> { element -> element.value }, sink)
            .to(HivePaths.forBeamIO(table.storage.location))
            // A partition name is itself a relative path under the table directory (dt=2024-01-01/hr=01), so splice it into the file name
            .withNaming { destination ->
                FileIO.Write.defaultNaming(
                    if (destination.isEmpty()) prefix else "$destination/$prefix",
                    extension,
                )
            }
        if (config.numShards > 0) {
            write = write.withNumShards(config.numShards)
        }

        val result = records.apply("WriteFiles", write)
        val mode = config.mode()
        if (mode == HiveWriteMode.APPEND && (!config.createPartitions || !table.partitioned)) {
            // Append write without partition registration: once the data is on disk there is nothing else to do
            return
        }
        result.perDestinationOutputFilenames
            // Group by partition: the commit step needs to know "which files this run wrote into this partition",
            // so that overwrite mode can delete the old files that are not in that set
            .apply("GroupByPartition", GroupByKey.create())
            .apply(
                "CommitPartitions",
                ParDo.of(
                    HiveCommitPartitionFn(
                        config.metastoreSpec(),
                        config.database,
                        config.table,
                        table.partitionColumns.map { it.name },
                        table.storage,
                        mode,
                        config.createPartitions,
                        config.hadoopConf,
                    )
                ),
            )
    }

    /** One token per job, fixed at graph construction time together with the transform, so job retries do not change it. */
    private fun runToken(): String = java.util.UUID.randomUUID().toString().replace("-", "").take(12)

    private fun checkWritable(table: HiveTable) {
        require(table.tableType != HiveTable.VIRTUAL_VIEW) {
            "${table.qualifiedName} is a view and cannot be written to"
        }
        require(table.parameters["transactional"]?.toBoolean() != true) {
            "${table.qualifiedName} is a transactional (ACID) table, writing is not supported yet: ACID tables write into a " +
                "delta directory and must maintain a write transaction number"
        }
        require(table.storage.location.isNotBlank()) {
            "${table.qualifiedName} has no location in the metastore, cannot tell where to write"
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

private val LOGGER = LoggerFactory.getLogger(HiveWriteProvider::class.java)
