package me.jayer.hdata.iceberg

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.iceberg.internal.parseSchemaFields
import me.jayer.hdata.iceberg.transform.IcebergReadFn
import org.apache.beam.sdk.transforms.Create
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
        val schemaFields = parseSchemaFields(config.schemaFields)
        val schema = config.outputSchema()
        val trigger = begin.apply("Trigger", Create.of(listOf("")))
        return trigger.apply("Read", ParDo.of(IcebergReadFn(config, schema, schemaFields))).setRowSchema(schema)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
