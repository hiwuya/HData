package me.jayer.hdata.iceberg.transform

import me.jayer.hdata.iceberg.IcebergReadConfig
import me.jayer.hdata.iceberg.internal.IcebergCatalogs
import me.jayer.hdata.iceberg.internal.recordToRow
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.apache.iceberg.Table
import org.apache.iceberg.data.IcebergGenerics
import org.apache.iceberg.hadoop.HadoopCatalog

/**
 * 从 Iceberg 表扫描全量（当前快照）并逐行映射成 Row。
 *
 * Catalog / Table 在每个 DoFn 实例里独立打开（`@Setup` 建、`@Teardown` 关），标记 `@Transient` 保证可序列化。
 *
 * @author wuya
 */
class IcebergReadFn(
    private val config: IcebergReadConfig,
    private val schema: Schema,
    private val schemaFields: List<Pair<String, Schema.FieldType>>,
) : DoFn<String, Row>() {

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
    fun processElement(@Element element: String, receiver: OutputReceiver<Row>) {
        val t = checkNotNull(table) { "Iceberg 表未初始化" }
        IcebergGenerics.read(t).build().use { records ->
            records.forEach { record -> receiver.output(recordToRow(schema, record, schemaFields)) }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
