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
 * The enumerator side of push-down aggregation: iterates the data files of the Iceberg table's current snapshot,
 * computes a partial aggregate [PartialAgg] per file, which is then merged globally into one row by
 * [org.apache.beam.sdk.transforms.Combine].
 *
 * With a `filter`, the predicate must be applied exactly as on the plain read path: first hand it to TableScan for
 * manifest-level pruning (files that do not match at all are cut away), then evaluate the residual predicate with
 * `Evaluator` on the rows of each surviving file — otherwise COUNT would take the whole file's recordCount as the
 * result and MIN/MAX/SUM/AVG would be computed over non-matching rows, so the job succeeds but the numbers are wrong.
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

    /** The residual evaluator for predicate push-down: evaluates the filter per row and accumulates only matching rows. */
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
        val t = checkNotNull(table) { "Iceberg table is not initialized" }
        // Aggregation does not need row-group splitting; one partial aggregate per data file suffices.
        // The filter first does manifest-level pruning (same semantics as the plain read path).
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
