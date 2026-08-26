package me.jayer.hdata.iceberg

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.iceberg.AggregateCombineFn
import me.jayer.hdata.iceberg.AggregateToRowFn
import me.jayer.hdata.iceberg.PartialAgg
import me.jayer.hdata.iceberg.aggregateSchema
import me.jayer.hdata.iceberg.internal.IcebergCatalogs
import me.jayer.hdata.iceberg.internal.parseSchemaFields
import me.jayer.hdata.iceberg.parseAggregations
import me.jayer.hdata.iceberg.transform.IcebergAggregateEnumeratorFn
import me.jayer.hdata.iceberg.transform.IcebergReadFileFn
import me.jayer.hdata.iceberg.transform.IcebergSplitEnumeratorFn
import org.apache.beam.sdk.transforms.Combine
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.coders.SerializableCoder
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `ReadFromIceberg`：扫描 Iceberg 表全量并逐行映射成 Row。
 *
 * @author wuya
 */
class IcebergReadProvider : TypedTransformProvider<IcebergReadConfig>(IcebergReadConfig::class.java) {

    override fun identifier(): String = "ReadFromIceberg"

    override fun description(): String = "从 Iceberg 读取"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(config: IcebergReadConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return IcebergSource(config)
    }
}

private class IcebergSource(private val config: IcebergReadConfig) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        // 聚合下推：COUNT/MIN/MAX 取自数据文件元数据统计，根本不读数据，输出聚合后的一行
        if (config.aggregations.isNotEmpty()) {
            val specs = parseAggregations(config.aggregations)
            val catalog = IcebergCatalogs.openCatalog(config.warehouse, config.catalogName)
            val table = IcebergCatalogs.loadTable(catalog, config.table)
            // 分区列的值记在 manifest 里、不在数据文件中，聚合枚举端不做分区回填——
            // 分区表在构图阶段就明确拒绝，别让用户收到晦涩的读取错误
            require(!table.spec().isPartitioned()) {
                "聚合下推暂不支持分区表[${config.table}]：分区列不在数据文件中，无法按文件局部聚合；请改用普通读取"
            }
            val outSchema = aggregateSchema(specs, table)
            runCatching { catalog.close() }
            val trigger = begin.apply("Trigger", Create.of(listOf("")))
            val partials = trigger.apply("EnumerateAgg", ParDo.of(IcebergAggregateEnumeratorFn(config, specs)))
            partials.setCoder(SerializableCoder.of(PartialAgg::class.java))
            val merged = partials.apply("MergeAgg", Combine.globally(AggregateCombineFn(specs)))
            merged.setCoder(SerializableCoder.of(PartialAgg::class.java))
            return merged.apply("ToRow", ParDo.of(AggregateToRowFn(specs, outSchema))).setRowSchema(outSchema)
        }
        val schemaFields = parseSchemaFields(config.schemaFields)
        val schema = config.outputSchema()
        val trigger = begin.apply("Trigger", Create.of(listOf("")))
        // 先枚举数据文件成 split（并行单元），再按文件并行读
        val splits = trigger.apply("EnumerateSplits", ParDo.of(IcebergSplitEnumeratorFn(config)))
        // limit 在 IcebergSplitEnumeratorFn 里退化为单 split、在 IcebergReadFileFn 里截断到 limit 行，
        // 与 JDBC/ES 的"限行数退化为单分区/单 slice"一致。
        return splits.apply("ReadFiles", ParDo.of(IcebergReadFileFn(config, schema, schemaFields))).setRowSchema(schema)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
