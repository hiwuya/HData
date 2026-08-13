package me.jayer.hdata.core.spi

import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollectionRowTuple

/**
 * 连接器扩展点：把 pipeline 文件里的一个 `type` 变成一个可执行的 Beam transform。
 *
 * 通过 `META-INF/services/me.jayer.hdata.core.spi.TransformProvider` 注册。
 *
 * 相比重构前的 `StructuredIOProvider`（一个类同时负责 createSource + createSink），
 * 这里一个 provider 只负责一个 `type`：只读的连接器不必再实现一个抛异常的 `createSink`，
 * 读写两端也可以各自演进配置，pipeline 文件里的名字（`ReadFromJdbc` / `WriteToJdbc`）
 * 也与 Beam YAML 的命名习惯一致。
 *
 * @author wuya
 * @date 2022-08-30
 */
interface TransformProvider {

    /** pipeline 文件里 `type` 的取值。 */
    fun identifier(): String

    fun description(): String = ""

    /**
     * 声明的输入端口。空表示读取端；[Tags.ANY] 表示接受任意数量的输入。
     */
    fun inputCollectionNames(): List<String> = listOf(Tags.MAIN_INPUT)

    /**
     * 声明的输出端口。空表示写入端（除非开启了 `error_handling`）。
     */
    fun outputCollectionNames(): List<String> = listOf(Tags.MAIN_OUTPUT)

    fun from(config: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple>
}

/**
 * 带配置类绑定的 [TransformProvider]，配置校验交给 Jackson + 配置类自身的 `init` / `validate`。
 */
abstract class TypedTransformProvider<C : Any>(private val configType: Class<C>) : TransformProvider {

    /**
     * @param config 已绑定的配置
     * @param context 框架级声明（`error_handling`、transform 名字等），多数连接器用不到
     */
    protected abstract fun create(config: C, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple>

    final override fun from(config: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> =
        create(config.bind(configType), config)
}
