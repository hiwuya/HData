package me.jayer.hdata.iceberg.transform

import me.jayer.hdata.iceberg.IcebergWriteConfig
import me.jayer.hdata.iceberg.internal.IcebergCatalogs
import me.jayer.hdata.iceberg.internal.schemaOf
import org.apache.beam.sdk.transforms.DoFn
import org.slf4j.LoggerFactory

/**
 * The one-time table clear used by `write_mode: overwrite`.
 *
 * Why it is a separate step: on the write side each bundle appends and commits its own data file, and if the table
 * clear were placed in [IcebergWriteFn], the second bundle would delete the data the first bundle just wrote —
 * leaving "overwrite" with only the last bundle's contents while the job status is still SUCCESS.
 *
 * Ordering it before all writes relies on a **side input**: a ParDo with a side input will not process any main input
 * until that side-input PCollection is fully computed. This is Beam's guarantee for bounded data, and the standard
 * "do A first, then B" pattern (see [me.jayer.hdata.iceberg.IcebergWriteProvider]).
 *
 * The table clear itself is idempotent (deleting again on an empty table does nothing), so bundle retries are safe.
 *
 * @author wuya
 */
// The trigger element uses String rather than Int: a non-null Kotlin Int parameter compiles to the primitive int,
// whereas Create.of produces a java.lang.Integer, and Beam's reflective DoFn type check would fail outright with
// "Type of @Element must match the DoFn type".
class IcebergTruncateFn(private val config: IcebergWriteConfig) : DoFn<String, String>() {

    @ProcessElement
    fun processElement(@Element element: String, receiver: OutputReceiver<String>) {
        IcebergCatalogs.openCatalog(config.warehouse, config.catalogName, config.hadoopConf).use { catalog ->
            val table = IcebergCatalogs.ensureTable(catalog, config.table, schemaOf(config.schemaFields))
            IcebergCatalogs.truncate(table)
            LOGGER.info("write_mode=overwrite: cleared Iceberg table {}", config.table)
        }
        receiver.output(element)
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(IcebergTruncateFn::class.java)
    }
}
