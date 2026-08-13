package me.jayer.hdata.core.transforms

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spi.HDataTransform
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TransformProvider
import org.apache.beam.sdk.transforms.Flatten
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollectionList
import org.apache.beam.sdk.values.PCollectionRowTuple

/**
 * 合并多路同 schema 的输入：
 *
 * ```yaml
 * - type: Flatten
 *   input: [ReadOrders, ReadArchivedOrders]
 * ```
 *
 * @author wuya
 * @date 2022-08-30
 */
class FlattenProvider : TransformProvider {

    override fun identifier(): String = "Flatten"

    override fun description(): String = "合并多路同 schema 的输入"

    override fun inputCollectionNames(): List<String> = listOf(Tags.ANY)

    override fun from(config: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        if (!config.isEmpty) {
            throw HDataException("transform[${config.transformName}] Flatten 不接受任何 config")
        }
        return FlattenRows()
    }
}

private class FlattenRows : HDataTransform() {

    override fun expand(input: PCollectionRowTuple): PCollectionRowTuple {
        val collections = input.all
        if (collections.isEmpty()) {
            throw HDataException("Flatten 至少需要一路输入")
        }
        val schemas = collections.mapValues { it.value.schema }
        val distinct = schemas.values.distinct()
        if (distinct.size > 1) {
            throw HDataException("Flatten 的各路输入 schema 必须一致，实际为: $schemas")
        }
        val merged = PCollectionList.of(collections.values).apply(Flatten.pCollections())
        if (!merged.hasSchema()) {
            merged.setRowSchema(distinct.single())
        }
        return PCollectionRowTuple.of(Tags.MAIN_OUTPUT, merged)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
