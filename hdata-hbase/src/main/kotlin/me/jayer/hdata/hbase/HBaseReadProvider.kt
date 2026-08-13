package me.jayer.hdata.hbase

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
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
 * `ReadFromHBase`：扫描 HBase 表，扫描本身直接复用 Beam 的 `HBaseReadSplittableDoFn`。
 *
 * 注意用的是 `HBaseIO.readAll()` 而不是 `HBaseIO.read()`——只有前者走 Splittable DoFn，
 * 后者内部还是老的 `BoundedSource`。
 *
 * 相比重构前自己写的那版 SDF：
 *  - 那版的限制固定是 `OffsetRange(0, 1)` 加 `tryClaim(0)`，**根本没有切分能力**，
 *    region 划分是在构图阶段一次性定死的，某个 region 数据倾斜就只能干等；
 *    Beam 这版用 `ByteKeyRangeTracker`，扫描过程中还能被运行时再切一刀分给空闲 worker。
 *  - 那版的 `Scan` 不带 `addColumn`，等于把每行的所有列族都拉下来再丢掉。
 *
 * @author wuya
 */
class HBaseReadProvider : TypedTransformProvider<HBaseReadConfig>(HBaseReadConfig::class.java) {

    override fun identifier(): String = "ReadFromHBase"

    override fun description(): String = "扫描 HBase 表，复用 Beam 的 HBaseReadSplittableDoFn 按 region 动态切分"

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
