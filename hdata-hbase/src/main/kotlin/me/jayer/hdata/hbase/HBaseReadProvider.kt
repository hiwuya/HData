package me.jayer.hdata.hbase

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.DeliveryCapabilities
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.OrderingScope
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.hbase.transform.HBaseResultToRowFn
import org.apache.beam.sdk.coders.SerializableCoder
import org.apache.beam.sdk.io.hbase.HBaseIO
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `ReadFromHBase` scans an HBase table through Beam's `HBaseReadSplittableDoFn`.
 *
 * It uses `HBaseIO.readAll()` rather than `HBaseIO.read()` because only the former uses a Splittable DoFn.
 *
 * Beam's implementation uses `ByteKeyRangeTracker`, so active scans can split regions dynamically at runtime.
 * It also projects configured columns through `addColumn`.
 *
 * @author wuya
 */
class HBaseReadProvider : TypedTransformProvider<HBaseReadConfig>(HBaseReadConfig::class.java) {

    override fun identifier(): String = "ReadFromHBase"

    override fun description(): String = "Scan HBase tables with Beam's region-splittable HBaseReadSplittableDoFn"

    override fun supportTier(): ConnectorSupportTier = ConnectorSupportTier.LOGIC_TESTED_ONLY

    override fun deliveryCapabilities(config: TransformConfig) = DeliveryCapabilities(
        DeliveryMode.AT_LEAST_ONCE, ReplayBehavior.FULL_REPLAY, OrderingScope.NONE,
        notes = "A bounded region scan has no HData-persisted position; a job restart can re-scan rows and regions have no global order.",
    )

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(
        config: HBaseReadConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return HBaseSource(config)
    }
}

private class HBaseSource(private val config: HBaseReadConfig) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        val codec = HBaseRowCodec.of(config.rowkeyField, config.rowkeyFormat, config.schemaFields, config.family)

        val read = HBaseIO.read()
            .withConfiguration(config.configuration())
            .withTableId(config.table)
            .withScan(config.scan())

        return begin
            .apply("Scan", Create.of(read).withCoder(SerializableCoder.of(HBaseIO.Read::class.java)))
            .apply("ReadFromHBase", HBaseIO.readAll())
            .apply("ToRow", ParDo.of(HBaseResultToRowFn(codec)))
            .setRowSchema(codec.schema)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
