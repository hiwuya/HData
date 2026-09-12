package me.jayer.hdata.debezium

import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.core.spi.DeliveryCapabilities
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.OrderingScope
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.core.spi.SourceMode
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

    override fun sourceMode(config: TransformConfig): SourceMode =
        if (config.bind(DebeziumReadConfig::class.java).maxRecords == null) SourceMode.UNBOUNDED else SourceMode.BOUNDED

    // EXPERIMENTAL: DebeziumMySqlContainerIT exercises this against a real MySQL binlog via Testcontainers,
    // including a restart/replay assertion (see docs/connectors.md#debezium) — more restart-safety coverage
    // than most connectors in this repository — but that IT test has not been executed in every environment
    // (no Docker daemon was available while writing it; see docs/MATURITY_ASSESSMENT.md), and there is no
    // dead-letter coverage or runner-qualification test beyond DirectRunner, so this does not yet meet the
    // full contract for QUALIFIED.
    override fun supportTier(): ConnectorSupportTier = ConnectorSupportTier.EXPERIMENTAL

    override fun deliveryCapabilities(config: TransformConfig): DeliveryCapabilities {
        val unbounded = config.bind(DebeziumReadConfig::class.java).maxRecords == null
        return DeliveryCapabilities(
            deliveryMode = DeliveryMode.AT_LEAST_ONCE,
            replayBehavior = if (unbounded) ReplayBehavior.RESUMABLE else ReplayBehavior.FULL_REPLAY,
            ordering = OrderingScope.GLOBAL,
            notes = if (unbounded) {
                "Persisted offset/schema-history state (validated by DebeziumReadConfig.validate(); see " +
                    "docs/connectors.md#debezium \"Crash and restart behavior\"): a restart resumes from the " +
                    "last flush, replaying only the window since then, never losing a record. A crash during " +
                    "the initial snapshot before any offset is persisted restarts the snapshot from scratch."
            } else {
                "max_records is set: a one-shot bounded read (typically for testing), not persisted CDC state; " +
                    "a restart re-runs the read (and, with snapshot_mode=initial, the snapshot) from scratch."
            },
        )
    }

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
