package me.jayer.hdata.hive.transform

import me.jayer.hdata.hive.split.HiveFile
import me.jayer.hdata.hive.split.HiveFileSystems
import me.jayer.hdata.hive.split.HivePartitionSpec
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.DoFn
import org.slf4j.LoggerFactory

/**
 * Lists the data files under each partition directory, mirroring Trino's `BackgroundHiveSplitLoader`.
 *
 * This lives in a DoFn rather than at graph construction time: on a table with thousands of partitions, listing them serially on
 * the submitter takes minutes, while the work is naturally parallel per partition.
 *
 * @author wuya
 */
class HiveListFilesFn(
    private val hadoopConf: Map<String, String>,
    private val recursive: Boolean,
) : DoFn<HivePartitionSpec, HiveFile>() {

    @ProcessElement
    fun processElement(@Element partition: HivePartitionSpec, receiver: OutputReceiver<HiveFile>) {
        val configuration = HiveFileSystems.configurationOf(hadoopConf)
        val files = HiveFileSystems.listFiles(configuration, partition.storage.location, recursive)
        files.forEach { status ->
            receiver.output(HiveFile(status.path.toString(), status.len, partition))
        }
        FILES_LISTED.inc(files.size.toLong())
        LOGGER.info(
            "partition[{}] directory {} has {} data files",
            partition.name.ifEmpty { "<non-partitioned table>" },
            partition.storage.location,
            files.size,
        )
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(HiveListFilesFn::class.java)
        private val FILES_LISTED = Metrics.counter(HiveListFilesFn::class.java, "files_listed")
    }
}
