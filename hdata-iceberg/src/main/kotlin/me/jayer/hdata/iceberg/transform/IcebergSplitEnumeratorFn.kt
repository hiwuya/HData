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
 * 枚举 Iceberg 表当前快照的数据文件，每个 [FileScanTask] 产出一个 [IcebergFileSplit]。
 *
 * Catalog / Table 在每个 DoFn 实例里独立打开（`@Setup` 建、`@Teardown` 关），标记 `@Transient` 保证可序列化。
 * 触发元素（`""`）只用来拉起一次枚举——整张表的 split 在单个 bundle 内一次性枚举完，之后各 split
 * 交给下游 [IcebergReadFileFn] 并行读。
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
        catalog = IcebergCatalogs.openCatalog(config.warehouse, config.catalogName)
        table = IcebergCatalogs.loadTable(catalog!!, config.table)
    }

    @Teardown
    fun teardown() {
        runCatching { catalog?.close() }
        catalog = null
    }

    @ProcessElement
    fun processElement(receiver: OutputReceiver<IcebergFileSplit>) {
        val t = checkNotNull(table) { "Iceberg 表未初始化" }
        val splitSize = config.splitSize
        // 过滤下推：把谓词交给 TableScan，Iceberg 在 manifest 层做分区/文件粒度裁剪，
        // 直接砍掉整文件不匹配的 split（读端再用 Evaluator 求每行残留谓词）。
        val scan = t.newScan()
        val files = if (config.filter.isNotBlank()) scan.filter(parseIcebergFilter(config.filter)) else scan
        files.planFiles().use { tasks: CloseableIterable<FileScanTask> ->
            tasks.forEach { task ->
                requireNoDeleteFiles(task, config.table)
                val file = task.file()
                require(file.format() == org.apache.iceberg.FileFormat.AVRO) {
                    "Iceberg 并行读取暂只支持 AVRO 数据文件，表[${config.table}]包含 ${file.format()} 文件: ${file.path()}；" +
                        "请先将数据文件改写为 AVRO"
                }
                val spec = task.spec()
                val partitionNames = spec.fields().map { it.name() }
                val partitionValues = spec.fields().mapIndexed { i, _ ->
                    serializablePartitionValue(task.partition().get(i, Any::class.java))
                }
                // 一个数据文件按 splitSize 细分成多个并行单元：大文件切到 row-group / 同步块粒度，
                // 小文件（<= splitSize）整文件一个 split。AVRO 按同步块切分，互不重叠、不重不漏。
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
