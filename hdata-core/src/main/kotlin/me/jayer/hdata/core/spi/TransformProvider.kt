package me.jayer.hdata.core.spi

import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollectionRowTuple

/**
 * The connector extension point: turns a `type` in the pipeline file into an executable Beam transform.
 *
 * Registered via `META-INF/services/me.jayer.hdata.core.spi.TransformProvider`.
 *
 * Compared to the pre-refactor `StructuredIOProvider` (one class handling both createSource + createSink),
 * here one provider handles only one `type`: a read-only connector no longer has to implement a
 * `createSink` that just throws, the read and write sides can evolve their configs independently, and
 * the names in the pipeline file (`ReadFromJdbc` / `WriteToJdbc`) are also consistent with Beam YAML's
 * naming conventions.
 *
 * @author wuya
 * @date 2022-08-30
 */
interface TransformProvider {

    /** The value of `type` in the pipeline file. */
    fun identifier(): String

    fun description(): String = ""

    /**
     * The declared input ports. Empty means a read side; [Tags.ANY] means it accepts any number of inputs.
     */
    fun inputCollectionNames(): List<String> = listOf(Tags.MAIN_INPUT)

    /**
     * The declared output ports. Empty means a write side (unless `error_handling` is enabled).
     */
    fun outputCollectionNames(): List<String> = listOf(Tags.MAIN_OUTPUT)

    fun from(config: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple>
}

/**
 * A [TransformProvider] with config-class binding; config validation is delegated to Jackson plus the config class's own `init` / `validate`.
 */
abstract class TypedTransformProvider<C : Any>(private val configType: Class<C>) : TransformProvider {

    /**
     * @param config the already-bound config
     * @param context framework-level declarations (`error_handling`, transform name, etc.); most connectors do not need it
     */
    protected abstract fun create(config: C, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple>

    final override fun from(config: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> =
        create(config.bind(configType), config)
}
