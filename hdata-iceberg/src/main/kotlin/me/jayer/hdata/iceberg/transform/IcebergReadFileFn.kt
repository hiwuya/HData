package me.jayer.hdata.iceberg.transform

import me.jayer.hdata.iceberg.IcebergReadConfig
import me.jayer.hdata.iceberg.internal.IcebergCatalogs
import me.jayer.hdata.iceberg.internal.recordToRow
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.apache.iceberg.FileFormat
import org.apache.iceberg.Table
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.Record
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.io.CloseableIterable

/**
 * 读单个 Iceberg 数据文件并逐行映射成 Row（[IcebergFileSplit] 是并行的基本单元）。
 *
 * 数据文件只存数据列；分区列的值记在 split 里，读完后按列名回填进完整 schema 的 Record，
 * 再交给 [recordToRow] 映射成 Beam Row。投影按"全表 schema 去掉分区列"读取，
 * 与旧实现（IcebergGenerics 读整表）在字段覆盖上等价，但现在是按文件并行。
 *
 * Catalog / Table 在每个 DoFn 实例里独立打开（`@Setup` 建、`@Teardown` 关），标记 `@Transient` 保证可序列化。
 *
 * @author wuya
 */
class IcebergReadFileFn(
    private val config: IcebergReadConfig,
    private val schema: Schema,
    private val schemaFields: List<Pair<String, Schema.FieldType>>,
) : DoFn<IcebergFileSplit, Row>() {

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
    fun processElement(@Element split: IcebergFileSplit, receiver: OutputReceiver<Row>) {
        val t = checkNotNull(table) { "Iceberg 表未初始化" }
        val fullSchema = t.schema()
        // 数据文件只存数据列，按"全表 schema 去掉分区列"投影读取；分区列按名字从 split 回填
        val partitionNames = split.partitionNames.toSet()
        val dataColumns = fullSchema.columns().filter { it.name() !in partitionNames }
        val dataSchema = org.apache.iceberg.Schema(dataColumns)

        val inputFile = t.io().newInputFile(split.path)
        val fileFormat = FileFormat.valueOf(split.format)
        // InternalData.read 与写入端 InternalData.write 对称，读出的是 Iceberg 的 GenericRecord，
        // 类型换算与分区回填都由本 DoFn 负责。当前构建的读取器只注册了 AVRO（与写入端固定 AVRO 一致）。
        val records: CloseableIterable<Record> = when (fileFormat) {
            FileFormat.AVRO -> org.apache.iceberg.InternalData.read(fileFormat, inputFile)
                .split(split.start, split.length).project(dataSchema).build<Record>()

            else -> throw UnsupportedOperationException(
                "Iceberg 数据文件格式[$fileFormat]当前构建未包含对应的读取器（仅支持 AVRO）",
            )
        }

        records.use { iterable ->
            iterable.forEach { dataRecord ->
                // 把数据列 + 分区列拼回完整 schema 的 Record，再映射成 Row
                val full = GenericRecord.create(fullSchema)
                fullSchema.columns().forEach { col ->
                    if (col.name() in partitionNames) {
                        val idx = split.partitionNames.indexOf(col.name())
                        full.setField(col.name(), split.partitionValues[idx])
                    } else {
                        full.setField(col.name(), dataRecord.getField(col.name()))
                    }
                }
                receiver.output(recordToRow(schema, full, schemaFields))
            }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
