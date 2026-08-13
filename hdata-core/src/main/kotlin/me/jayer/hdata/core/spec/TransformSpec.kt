package me.jayer.hdata.core.spec

import me.jayer.hdata.core.exception.HDataException
import tools.jackson.databind.JsonNode
import tools.jackson.databind.node.JsonNodeFactory
import tools.jackson.databind.node.ObjectNode

/**
 * 单个 transform 的声明，对齐 Beam YAML 的 transform 结构：
 *
 * ```yaml
 * - type: MapToFields
 *   name: Rename
 *   input: ReadFromJdbc
 *   config:
 *     fields: { id: c_id }
 *     error_handling: { output: errors }
 * ```
 *
 * 当 [type] 为 `chain` 或 `composite` 时该节点是复合 transform，
 * 由 [transforms] / [source] / [sink] / [extraTransforms] 描述子图。
 *
 * @author wuya
 * @date 2022-08-30
 */
data class TransformSpec(
    /** 省略时按是否含子节点推断为 [COMPOSITE]，见 [kind]。 */
    val type: String = "",
    val name: String? = null,
    /** 单输入写 `input: Foo`；多输入写 `input: {A: Foo, B: Bar}` 或 `input: [Foo, Bar]`。 */
    val input: JsonNode? = null,
    /** [input] 的别名，语义完全一致。 */
    val inputs: JsonNode? = null,
    val config: JsonNode? = null,
    /** 复合 transform 的子节点。 */
    val transforms: List<TransformSpec> = emptyList(),
    /** 复合 transform 的首节点简写。 */
    val source: TransformSpec? = null,
    /** 复合 transform 的尾节点简写。 */
    val sink: TransformSpec? = null,
    /** 在 chain 之外追加的节点，可以引用 chain 内部任意名字（典型用途是消费错误流）。 */
    val extraTransforms: List<TransformSpec> = emptyList(),
    val windowing: WindowingSpec? = null,
    /** 复合 transform 的输出，取值为内部某个节点的引用。 */
    val output: JsonNode? = null,
) {

    companion object {
        const val CHAIN = "chain"
        const val COMPOSITE = "composite"

        /** 未显式命名输入端口时使用的占位 key，由框架按 provider 声明的端口名回填。 */
        const val DEFAULT_INPUT_KEY = ""
    }

    /** DAG 中的节点名，未显式声明时退化为 [type]。 */
    val displayName: String get() = name ?: type.ifBlank { "<unnamed>" }

    /** 生效的 transform 类型：显式 [type] 优先，含子节点时默认为 [COMPOSITE]。 */
    val kind: String
        get() = when {
            type.isNotBlank() -> type
            hasChildren() -> COMPOSITE
            else -> throw HDataException("transform 缺少 type 字段: ${name?.let { "name=$it" } ?: this}")
        }

    val composite: Boolean get() = kind == CHAIN || kind == COMPOSITE

    val chain: Boolean get() = kind == CHAIN

    private fun hasChildren(): Boolean =
        source != null || sink != null || transforms.isNotEmpty() || extraTransforms.isNotEmpty()

    /** 复合节点的子节点，按 source -> transforms -> sink 顺序展开。 */
    fun children(): List<TransformSpec> =
        buildList {
            source?.let { add(it) }
            addAll(transforms)
            sink?.let { add(it) }
        }

    fun configNode(): ObjectNode =
        when {
            config == null || config.isNull -> JsonNodeFactory.instance.objectNode()
            config is ObjectNode -> config
            else -> throw HDataException("Transform[$displayName] 的 config 必须是对象，实际为: ${config.nodeType}")
        }

    /**
     * 解析输入引用。key 为输入端口名，[DEFAULT_INPUT_KEY] 表示"由 provider 决定端口名"。
     * value 为引用串，形如 `TransformName` 或 `TransformName.outputTag`。
     */
    fun inputRefs(): Map<String, String> {
        if (input != null && inputs != null) {
            throw HDataException("Transform[$displayName] 不能同时声明 input 和 inputs")
        }
        val node = input ?: inputs ?: return emptyMap()
        return when {
            node.isNull -> emptyMap()
            node.isString -> mapOf(DEFAULT_INPUT_KEY to node.stringValue())
            node.isArray -> node.mapIndexed { index, element -> index.toString() to refOf(element) }.toMap()
            node.isObject -> node.properties().associate { (key, value) -> key to refOf(value) }
            else -> throw HDataException("Transform[$displayName] 的 input 必须是字符串、数组或对象，实际为: ${node.nodeType}")
        }
    }

    /** 复合节点的输出引用，语义与 [inputRefs] 相同。 */
    fun outputRefs(): Map<String, String> {
        val node = output ?: return emptyMap()
        return when {
            node.isNull -> emptyMap()
            node.isString -> mapOf(DEFAULT_INPUT_KEY to node.stringValue())
            node.isObject -> node.properties().associate { (key, value) -> key to refOf(value) }
            else -> throw HDataException("Transform[$displayName] 的 output 必须是字符串或对象，实际为: ${node.nodeType}")
        }
    }

    private fun refOf(node: JsonNode): String =
        if (node.isString) node.stringValue()
        else throw HDataException("Transform[$displayName] 的输入/输出引用必须是字符串，实际为: ${node.nodeType}")
}
