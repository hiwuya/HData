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
 * `ReadFromDebezium` — a CDC source based on the Debezium embedded engine.
 *
 * The read side can be graphed without connecting to the database: the output schema is fixed as
 * [DebeziumRecords.SCHEMA], determined by config + type binding; it only requires that the config can be JSON-bound and
 * [DebeziumReadConfig.validate] passes, with the actual database connection happening at runtime.
 */
class DebeziumReadProvider : TypedTransformProvider<DebeziumReadConfig>(DebeziumReadConfig::class.java) {

    override fun identifier(): String = "ReadFromDebezium"

    override fun description(): String = "A CDC source based on the Debezium embedded engine (binlog / WAL change capture)"

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
