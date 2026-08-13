package me.jayer.hdata.hbase

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.hbase.transform.HBaseReadFn
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory

/**
 * `ReadFromHBase`：按 region 边界切分并行读 HBase，使用 Splittable DoFn。
 *
 * region 边界在构图阶段通过 [HBaseConnections.listRegions] 拿到，所以提交作业的机器需要能连上 HBase。
 */
class HBaseReadProvider : TypedTransformProvider<HBaseReadConfig>(HBaseReadConfig::class.java) {

    override fun identifier(): String = "ReadFromHBase"

    override fun description(): String = "按 region 边界切分并行读 HBase，使用 Splittable DoFn"

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
        val regions = HBaseConnections.listRegions(config.zookeeperQuorum, config.table)
        LOGGER.info("ReadFromHBase 表[{}] 共 {} 个 region", config.table, regions.size)
        val schema = buildReadSchema(config.rowkeyField, parseSchemaFields(config.schemaFields))
        return begin.apply("Regions", Create.of(regions))
            .apply(
                "Read",
                ParDo.of(
                    HBaseReadFn(
                        config.zookeeperQuorum,
                        config.rowkeyField,
                        config.family,
                        config.schemaFields,
                        config.scanCaching,
                    )
                ),
            )
            .setRowSchema(schema)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

private val LOGGER = LoggerFactory.getLogger(HBaseReadProvider::class.java)
