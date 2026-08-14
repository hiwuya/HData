package me.jayer.hdata.hive.transform

import me.jayer.hdata.hive.split.HiveFile
import me.jayer.hdata.hive.split.HiveFileSystems
import me.jayer.hdata.hive.split.HivePartitionSpec
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.DoFn
import org.slf4j.LoggerFactory

/**
 * 列出每个分区目录下的数据文件，对应 Trino 的 `BackgroundHiveSplitLoader`。
 *
 * 放在 DoFn 里而不是构图阶段：分区数上千的表，在提交端串行 list 一遍要几分钟，
 * 而这件事天然可以按分区并行。
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
            "分区[{}] 目录 {} 下有 {} 个数据文件",
            partition.name.ifEmpty { "<非分区表>" },
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
