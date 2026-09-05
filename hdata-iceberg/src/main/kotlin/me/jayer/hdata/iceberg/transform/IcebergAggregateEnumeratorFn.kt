package me.jayer.hdata.iceberg.transform

import me.jayer.hdata.iceberg.AggSpec
import me.jayer.hdata.iceberg.IcebergReadConfig
import me.jayer.hdata.iceberg.PartialAgg
import me.jayer.hdata.iceberg.internal.IcebergCatalogs
import me.jayer.hdata.iceberg.internal.requireNoDeleteFiles
import me.jayer.hdata.iceberg.parseIcebergFilter
import me.jayer.hdata.iceberg.partialAggFromTask
import org.apache.beam.sdk.transforms.DoFn
import org.apache.iceberg.Table
import org.apache.iceberg.expressions.Evaluator
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.io.CloseableIterable

/**
 * 聚合下推的枚举端：遍历 Iceberg 表当前快照的数据文件，每个文件算出局部聚合 [PartialAgg]，
 * 之后由 [org.apache.beam.sdk.transforms.Combine] 全局合并成一行。
 *
 * 带 `filter` 时必须与普通读路径一致地应用谓词：先交给 TableScan 做 manifest 级裁剪（整文件不匹配的直接砍掉），
 * 再对每个幸存文件里的行用 `Evaluator` 求残留谓词——否则 COUNT 会把整文件的 recordCount 当结果、
 * MIN/MAX/SUM/AVG 会在不匹配的行上算，作业成功但数字是错的。
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

    /** 谓词下推的残留求值器：对每行求 filter，只累计匹配的行。 */
    @Transient
    private var evaluator: Evaluator? = null

    @Setup
    fun setup() {
        catalog = IcebergCatalogs.openCatalog(config.warehouse, config.catalogName)
        table = IcebergCatalogs.loadTable(catalog!!, config.table)
        if (config.filter.isNotBlank()) {
            evaluator = Evaluator(checkNotNull(table).schema().asStruct(), parseIcebergFilter(config.filter), true)
        }
    }

    @Teardown
    fun teardown() {
        runCatching { catalog?.close() }
        catalog = null
    }

    @ProcessElement
    fun processElement(receiver: OutputReceiver<PartialAgg>) {
        val t = checkNotNull(table) { "Iceberg 表未初始化" }
        // 聚合不需要 row-group 切分，每个数据文件一个局部聚合即可；
        // filter 先做 manifest 级裁剪（与普通读路径同一套语义）
        val scan = t.newScan()
        val tasks: CloseableIterable<org.apache.iceberg.FileScanTask> =
            if (config.filter.isNotBlank()) scan.filter(parseIcebergFilter(config.filter)).planFiles() else scan.planFiles()
        tasks.use {
            it.forEach { task ->
                requireNoDeleteFiles(task, config.table)
                receiver.output(partialAggFromTask(task, specs, t, evaluator))
            }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
