package me.jayer.hdata.core.spi

/**
 * The port names agreed upon within [org.apache.beam.sdk.values.PCollectionRowTuple].
 *
 * Consistent with Beam `SchemaTransform`'s convention: a single input is called `input` and a single output is called `output`.
 *
 * @author wuya
 * @date 2022-08-30
 */
object Tags {

    const val MAIN_INPUT = "input"

    const val MAIN_OUTPUT = "output"

    /** The dead-letter output port, used together with `error_handling` in the config. */
    const val ERROR_OUTPUT = "errors"

    /**
     * Declared in [TransformProvider.inputCollectionNames] to mean "accepts any number of arbitrarily named inputs",
     * used by variadic transforms such as Flatten.
     */
    const val ANY = "*"
}
