package me.jayer.hdata.iceberg.transform

import me.jayer.hdata.iceberg.IcebergReadConfig
import me.jayer.hdata.iceberg.internal.IcebergCatalogs
import me.jayer.hdata.iceberg.internal.recordToRow
import me.jayer.hdata.iceberg.internal.validateReadableSchema
import me.jayer.hdata.iceberg.parseIcebergFilter
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.apache.iceberg.FileFormat
import org.apache.iceberg.Table
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.Record
import org.apache.iceberg.expressions.Evaluator
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.io.CloseableIterable

/**
 * 读单个 Iceberg 数据文件并逐行映射成 Row（[IcebergFileSplit] 是并行的基本单元）。
 *
 * 分区值记在 split 里，读完后按列名回填进完整 schema 的 Record，再交给 [recordToRow]
 * 映射成 Beam Row。没有残留谓词时只投影输出列；有谓词时读取整表列供 [Evaluator] 求值。
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

    /** 谓词下推的残留求值器：对每行（数据列 + 分区列）求 filter，过滤掉不匹配的行。 */
    @Transient
    private var evaluator: Evaluator? = null

    @Setup
    fun setup() {
        catalog = IcebergCatalogs.openCatalog(config.warehouse, config.catalogName)
        table = IcebergCatalogs.loadTable(catalog!!, config.table)
        validateReadableSchema(checkNotNull(table).schema(), schemaFields, config.table)
        // filter 已经交给 TableScan 做了 manifest 级裁剪；这里对每行再求一次（Evaluator 基于整表
        // schema，分区列与数据列都能正确判定），保证下推的谓词真正生效、不静默漏过滤。
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
    fun processElement(@Element split: IcebergFileSplit, receiver: OutputReceiver<Row>) {
        val t = checkNotNull(table) { "Iceberg 表未初始化" }
        val fullSchema = t.schema()
        val partitionNames = split.partitionNames.toSet()
        val dataSchema = icebergReadProjection(
            fullSchema = fullSchema,
            outputFieldNames = schemaFields.map { it.first }.toSet(),
            partitionNames = partitionNames,
            requiresFilterEvaluation = evaluator != null,
        )

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
            val it = iterable.iterator()
            while (it.hasNext()) {
                val dataRecord = it.next()
                // 只给投影出来的列赋值；其余未读取列保持 null，recordToRow 不会访问它们。
                val full = GenericRecord.create(fullSchema)
                dataSchema.columns().forEach { col ->
                    full.setField(col.name(), dataRecord.getField(col.name()))
                }
                split.partitionNames.forEachIndexed { index, name ->
                    val col = fullSchema.findField(name) ?: return@forEachIndexed
                    full.setField(name, icebergPartitionValue(split.partitionValues[index], col.type()))
                }
                // 谓词下推的残留过滤：不匹配的行直接丢弃（分区列与数据列都在 full 里）
                if (evaluator != null && !evaluator!!.eval(full)) continue
                receiver.output(recordToRow(schema, full, schemaFields))
            }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/** 过滤求值需要完整行；普通读取只取输出列，并由 split 回填 identity 分区字段。 */
internal fun icebergReadProjection(
    fullSchema: org.apache.iceberg.Schema,
    outputFieldNames: Set<String>,
    partitionNames: Set<String>,
    requiresFilterEvaluation: Boolean,
): org.apache.iceberg.Schema {
    val requiredNames = if (requiresFilterEvaluation) {
        fullSchema.columns().mapTo(linkedSetOf()) { it.name() }
    } else {
        outputFieldNames
    }
    return org.apache.iceberg.Schema(
        fullSchema.columns().filter { it.name() in requiredNames && it.name() !in partitionNames },
    )
}
