package me.jayer.hdata.iceberg.transform

import me.jayer.hdata.iceberg.AggSpec
import me.jayer.hdata.iceberg.IcebergReadConfig
import me.jayer.hdata.iceberg.PartialAgg
import me.jayer.hdata.iceberg.internal.IcebergCatalogs
import me.jayer.hdata.iceberg.partialAggFromTask
import org.apache.beam.sdk.transforms.DoFn
import org.apache.iceberg.Table
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.io.CloseableIterable

/**
 * 聚合下推的枚举端：遍历 Iceberg 表当前快照的数据文件，每个文件直接从元数据统计
 * （recordCount / lower_bounds / upper_bounds）算出局部聚合 [PartialAgg]，根本不读数据文件。
 * 之后由 [org.apache.beam.sdk.transforms.Combine] 全局合并成一行。
 *
 * @author wuya
 */
class IcebergAggregateEnumeratorFn(
    private val config: IcebergReadConfig,
    private val specs: List<AggSpec>,
) : DoFn<String, PartialAgg>() {

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
    fun processElement(receiver: OutputReceiver<PartialAgg>) {
        val t = checkNotNull(table) { "Iceberg 表未初始化" }
        // 聚合不需要 row-group 切分，每个数据文件一个局部聚合即可
        t.newScan().planFiles().use { tasks: CloseableIterable<org.apache.iceberg.FileScanTask> ->
            tasks.forEach { task ->
                receiver.output(partialAggFromTask(task, specs, t))
            }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
