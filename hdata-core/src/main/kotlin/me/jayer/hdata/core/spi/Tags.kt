package me.jayer.hdata.core.spi

/**
 * [org.apache.beam.sdk.values.PCollectionRowTuple] 中约定的端口名。
 *
 * 与 Beam `SchemaTransform` 的惯例保持一致：单输入叫 `input`，单输出叫 `output`。
 *
 * @author wuya
 * @date 2022-08-30
 */
object Tags {

    const val MAIN_INPUT = "input"

    const val MAIN_OUTPUT = "output"

    /** 死信输出端口，配合 config 里的 `error_handling` 使用。 */
    const val ERROR_OUTPUT = "errors"

    /**
     * 声明在 [TransformProvider.inputCollectionNames] 里表示"接受任意数量、任意命名的输入"，
     * 供 Flatten 这类可变元 transform 使用。
     */
    const val ANY = "*"
}
