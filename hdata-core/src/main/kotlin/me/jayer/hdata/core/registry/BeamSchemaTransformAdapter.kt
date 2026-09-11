package me.jayer.hdata.core.registry

import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TransformProvider
import me.jayer.hdata.core.util.RowConverters
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollectionRowTuple

/**
 * Plugs Beam's native [SchemaTransformProvider]s found on the classpath into HData's registry.
 *
 * Because HData's transform contract *is* Beam's `SchemaTransform`, this adapter layer only has to do
 * one thing: convert the config syntax tree into a config [org.apache.beam.sdk.values.Row] according
 * to the `configurationSchema()` declared by the provider. As a result, off-the-shelf IOs from the Beam
 * ecosystem (e.g. `beam:schematransform:org.apache.beam:jdbc_read:v1`) can be written directly into a
 * pipeline file without having to write an HData connector for each one.
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
