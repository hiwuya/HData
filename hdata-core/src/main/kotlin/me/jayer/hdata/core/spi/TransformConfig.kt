package me.jayer.hdata.core.spi

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spec.ErrorHandlingSpec
import me.jayer.hdata.core.spec.SpecMappers
import tools.jackson.databind.node.ObjectNode

/**
 * 交给连接器的配置视图。
 *
 * 这里刻意只暴露格式无关的语法树，连接器不必知道 pipeline 文件长什么样，
 * 也就不会像重构前那样在实现里直接依赖 `TomlMapper`。
 *
 * `error_handling` 已由框架摘走，通过 [errorHandling] 暴露，因此配置类可以安全地开启
 * `FAIL_ON_UNKNOWN_PROPERTIES` 来捕获拼写错误。
 *
 * @author wuya
 * @date 2022-08-30
 */
class TransformConfig(
    /** transform 在 DAG 中的名字，仅用于报错定位。 */
    val transformName: String,
    private val node: ObjectNode,
    val errorHandling: ErrorHandlingSpec? = null,
) {

    val raw: ObjectNode get() = node

    val isEmpty: Boolean get() = node.isEmpty

    /** 把配置绑定到连接器自己的配置类上，键按 `snake_case` 匹配。 */
    fun <T : Any> bind(type: Class<T>): T = try {
        SpecMappers.CONFIG.treeToValue(node, type)
    } catch (e: Exception) {
        throw HDataException("transform[$transformName] 的 config 无效: ${e.message}", e)
    }

    override fun toString(): String = "TransformConfig(transform=$transformName, keys=${node.propertyNames().toList()})"
}
