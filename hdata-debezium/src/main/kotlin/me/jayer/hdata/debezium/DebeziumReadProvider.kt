package me.jayer.hdata.debezium

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.debezium.internal.DebeziumRecords
import me.jayer.hdata.debezium.transform.DebeziumReadFn
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `ReadFromDebezium` —— 基于 Debezium 嵌入式引擎的 CDC 源。
 *
 * 读端不连库即可构图：输出 schema 固定为 [DebeziumRecords.SCHEMA]，由配置 + 类型绑定决定，
 * 只要配置能被 JSON 绑定、[DebeziumReadConfig.validate] 能过即可，真正连库发生在运行时。
 */
class DebeziumReadProvider : TypedTransformProvider<DebeziumReadConfig>(DebeziumReadConfig::class.java) {

    override fun identifier(): String = "ReadFromDebezium"

    override fun description(): String = "基于 Debezium 嵌入式引擎的 CDC 源（binlog / WAL 变更捕获）"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(config: DebeziumReadConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return DebeziumSource(config)
    }
}

private class DebeziumSource(private val config: DebeziumReadConfig) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        val trigger = begin.apply("Trigger", Create.of(listOf("")))
        return trigger.apply("Read", ParDo.of(DebeziumReadFn(config)))
            .setRowSchema(DebeziumRecords.SCHEMA)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
