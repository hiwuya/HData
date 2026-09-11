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
 * The commit step after the data hits disk, mirroring Trino's `HiveMetadata.finishInsert`: in overwrite mode it clears the old
 * files in the partition and then registers new partitions in the metastore.
 *
 * The input is what `FileIO`'s `getPerDestinationOutputFilenames()` grouped by partition, i.e. "which files this run wrote into
 * this partition". Therefore:
 *  - partitions that received no data **never appear in the input**, so overwrite mode cannot hurt them (consistent with Hive's
 *    dynamic partition overwrite semantics);
 *  - deletion excludes the files written this run by name, rather than relying on unreliable criteria such as timestamps.
 *
 * This step is **idempotent**: when a bundle retries, the old files are already gone and the files written this run are still in the set, so they are not deleted.
 *
 * Note that overwrite is not atomic: in the short window where the new files have been renamed into place but the old ones are
 * not fully deleted yet, a reader sees both sets of data. A non-ACID Hive table is like this anyway (Hive itself is too); strong
 * consistency requires a transactional table.
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

    // @JvmSuppressWildcards is required: Kotlin's Iterable<out E> compiles to the Java signature Iterable<? extends String>,
    // while GroupByKey produces Iterable<String>, and Beam compares the DoFn input type by reflection, failing outright with
    // "Type of @Element must match the DoFn type".

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
        // Beam reports full paths, but they have already been stripped of the scheme by HivePaths.forBeamIO, while the paths
        // listed by Hadoop carry the scheme, so the two can only be compared by file name
        val written = element.value.map { Path(it).name }.toSet()

        if (writeMode == HiveWriteMode.OVERWRITE) {
            removeStaleFiles(partitionName, written)
        }
        if (partitionName.isNotEmpty() && createPartitions) {
            addPartition(partitionName)
        }
        receiver.output(partitionName)
    }

    /** Deletes the data files in this partition directory that were **not written by this run**. */
    private fun removeStaleFiles(partitionName: String, written: Set<String>) {
        val configuration = HiveFileSystems.configurationOf(hadoopConf)
        val location = partitionLocation(partitionName)
        // Not recursive: a subdirectory under a partition directory can only be ACID's delta/base, and such tables are already
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
                throw IllegalStateException("cannot delete old files during an overwrite write: ${status.path}")
            }
        }
        LOGGER.info(
            "overwrite write: partition[{}] deleted {} old files, kept the {} written this run",
            partitionName.ifEmpty { "<non-partitioned table>" },
            stale.size,
            written.size,
        )
    }

    private fun addPartition(partitionName: String) {
        val values = PartitionNames.toPartitionValues(partitionName)
        require(values.size == partitionColumnNames.size) {
            "the number of values in partition name [$partitionName] does not match the partition columns $partitionColumnNames"
        }
        val partition = HivePartition(
            values = values,
            storage = tableStorage.copy(location = partitionLocation(partitionName)),
        )
        val added = checkNotNull(metastore) { "metastore client is not initialized" }
            .addPartitions(databaseName, tableName, mapOf(partitionName to partition))
        if (added.isNotEmpty()) {
            PARTITIONS_ADDED.inc()
            LOGGER.info("registered new partition {}.{} {}", databaseName, tableName, partitionName)
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
