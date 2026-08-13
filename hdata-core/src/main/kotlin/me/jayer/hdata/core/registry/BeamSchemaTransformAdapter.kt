package me.jayer.hdata.core.registry

import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TransformProvider
import me.jayer.hdata.core.util.RowConverters
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollectionRowTuple

/**
 * 把 classpath 上的 Beam 原生 [SchemaTransformProvider] 接进 HData 的注册表。
 *
 * 因为 HData 的 transform 契约就是 Beam 的 `SchemaTransform`，这层适配只需要做一件事：
 * 把配置语法树按 provider 声明的 `configurationSchema()` 转成配置 [org.apache.beam.sdk.values.Row]。
 * 于是 Beam 生态里现成的 IO（例如 `beam:schematransform:org.apache.beam:jdbc_read:v1`）
 * 可以直接写进 pipeline 文件，不必为每个都写一遍 HData 连接器。
 *
 * @author wuya
 * @date 2022-08-30
 */
class BeamSchemaTransformAdapter(private val delegate: SchemaTransformProvider) : TransformProvider {

    override fun identifier(): String = delegate.identifier()

    override fun description(): String = delegate.description()

    override fun inputCollectionNames(): List<String> = delegate.inputCollectionNames()

    override fun outputCollectionNames(): List<String> = delegate.outputCollectionNames()

    override fun from(config: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        val configurationRow = RowConverters.toRow(
            delegate.configurationSchema(),
            config.raw,
            "transform[${config.transformName}].config",
        )
        return delegate.from(configurationRow)
    }
}
