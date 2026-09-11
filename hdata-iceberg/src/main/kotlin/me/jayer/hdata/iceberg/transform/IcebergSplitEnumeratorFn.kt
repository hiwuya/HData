package me.jayer.hdata.iceberg.transform

import me.jayer.hdata.iceberg.IcebergReadConfig
import me.jayer.hdata.iceberg.internal.IcebergCatalogs
import me.jayer.hdata.iceberg.internal.requireNoDeleteFiles
import me.jayer.hdata.iceberg.parseIcebergFilter
import org.apache.beam.sdk.transforms.DoFn
import org.apache.iceberg.FileScanTask
import org.apache.iceberg.Table
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.io.CloseableIterable

/**
 * Enumerates the data files of the Iceberg table's current snapshot, producing one [IcebergFileSplit] per
 * [FileScanTask].
 *
 * The Catalog / Table are opened independently in each DoFn instance (created in `@Setup`, closed in `@Teardown`), and
 * marked `@Transient` to keep it serializable. The trigger element (`""`) is only there to kick off enumeration once —
 * the whole table's splits are enumerated in one go within a single bundle, after which each split is read in parallel
 * by the downstream [IcebergReadFileFn].
 *
 * @author wuya
 */
class IcebergSplitEnumeratorFn(private val config: IcebergReadConfig) : DoFn<String, IcebergFileSplit>() {

    @Transient
    private var catalog: HadoopCatalog? = null

    @Transient
    private var table: Table? = null

    @Setup
    fun setup() {
        catalog = IcebergCatalogs.openCatalog(config.warehouse, config.catalogName, config.hadoopConf)
        table = IcebergCatalogs.loadTable(catalog!!, config.table)
    }

    @Teardown
    fun teardown() {
        runCatching { catalog?.close() }
        catalog = null
    }

    @ProcessElement
    fun processElement(receiver: OutputReceiver<IcebergFileSplit>) {
        val t = checkNotNull(table) { "Iceberg table is not initialized" }
        val splitSize = config.splitSize
        // Filter push-down: hand the predicate to TableScan, and Iceberg prunes at partition/file granularity in the
        // manifest layer, cutting away splits whose entire file does not match (the read side then evaluates the
        // per-row residual predicate with Evaluator).
        val scan = t.newScan()
        val files = if (config.filter.isNotBlank()) scan.filter(parseIcebergFilter(config.filter)) else scan
        files.planFiles().use { tasks: CloseableIterable<FileScanTask> ->
            tasks.forEach { task ->
                requireNoDeleteFiles(task, config.table)
                val file = task.file()
                require(file.format() == org.apache.iceberg.FileFormat.AVRO) {
                    "Iceberg parallel reads currently support only AVRO data files, but table [${config.table}] contains a ${file.format()} file: ${file.path()}; " +
                        "please rewrite the data files as AVRO first"
                }
                val spec = task.spec()
                val partitionNames = spec.fields().map { it.name() }
                val partitionValues = spec.fields().mapIndexed { i, _ ->
                    serializablePartitionValue(task.partition().get(i, Any::class.java))
                }
                // A single data file is subdivided into multiple parallel units by splitSize: large files are cut to
                // row-group / sync-block granularity, while small files (<= splitSize) get one split for the whole
                // file. AVRO is split on sync blocks, so splits never overlap and nothing is duplicated or lost.
                val fileStart = task.start()
                val fileLen = task.length()
                val chunks = if (fileLen <= 0) 1L else (fileLen + splitSize - 1) / splitSize
                for (i in 0 until chunks) {
                    val subStart = fileStart + i * splitSize
                    val remain = fileLen - i * splitSize
                    val subLen = if (remain < splitSize) remain else splitSize
                    receiver.output(
                        IcebergFileSplit(
                            path = file.path().toString(),
                            format = file.format().name,
                            start = subStart,
                            length = subLen,
                            partitionSpecId = spec.specId(),
                            partitionNames = partitionNames,
                            partitionValues = partitionValues,
                        ),
                    )
                }
            }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
