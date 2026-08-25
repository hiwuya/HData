package me.jayer.hdata.iceberg.transform

import me.jayer.hdata.iceberg.IcebergReadConfig
import me.jayer.hdata.iceberg.internal.IcebergCatalogs
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
        t.newScan().planFiles().use { tasks: CloseableIterable<FileScanTask> ->
            tasks.forEach { task ->
                val file = task.file()
                val spec = task.spec()
                val partitionNames = spec.fields().map { it.name() }
                val partitionValues = spec.fields().mapIndexed { i, _ -> task.partition().get(i, Any::class.java) }
                receiver.output(
                    IcebergFileSplit(
                        path = file.path().toString(),
                        format = file.format().name,
                        start = task.start(),
                        length = task.length(),
                        partitionSpecId = spec.specId(),
                        partitionNames = partitionNames,
                        partitionValues = partitionValues,
                    ),
                )
            }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
