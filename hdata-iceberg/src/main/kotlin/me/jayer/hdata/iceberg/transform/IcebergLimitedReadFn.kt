package me.jayer.hdata.iceberg.transform

import me.jayer.hdata.iceberg.IcebergReadConfig
import me.jayer.hdata.iceberg.internal.IcebergCatalogs
import me.jayer.hdata.iceberg.internal.recordToRow
import me.jayer.hdata.iceberg.internal.validateReadableSchema
import me.jayer.hdata.iceberg.parseIcebergFilter
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.apache.iceberg.Table
import org.apache.iceberg.data.IcebergGenerics
import org.apache.iceberg.hadoop.HadoopCatalog

/**
 * Iceberg 的全局 LIMIT 读取路径。一个触发元素由一个 worker 顺序扫描当前快照，跨越任意数量的数据文件，
 * 在真正产出 [IcebergReadConfig.limit] 条匹配记录后停止。GenericReader 同时负责应用 equality/position
 * deletes，避免“只读第一个文件”导致不足 limit 或返回已删除记录。
 */
class IcebergLimitedReadFn(
    private val config: IcebergReadConfig,
    private val schema: Schema,
    private val schemaFields: List<Pair<String, Schema.FieldType>>,
) : DoFn<String, Row>() {

    @Transient private var catalog: HadoopCatalog? = null
    @Transient private var table: Table? = null

    @Setup
    fun setup() {
        catalog = IcebergCatalogs.openCatalog(config.warehouse, config.catalogName)
        table = IcebergCatalogs.loadTable(checkNotNull(catalog), config.table)
        validateReadableSchema(checkNotNull(table).schema(), schemaFields, config.table)
    }

    @Teardown
    fun teardown() {
        runCatching { catalog?.close() }
        catalog = null
        table = null
    }

    @ProcessElement
    fun processElement(receiver: OutputReceiver<Row>) {
        val t = checkNotNull(table) { "Iceberg 表未初始化" }
        var builder = IcebergGenerics.read(t)
        if (config.filter.isNotBlank()) builder = builder.where(parseIcebergFilter(config.filter))
        builder.build().use { records ->
            val iterator = records.iterator()
            var emitted = 0L
            while (emitted < config.limit && iterator.hasNext()) {
                receiver.output(recordToRow(schema, iterator.next(), schemaFields))
                emitted++
            }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
