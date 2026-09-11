package me.jayer.hdata.core.spec

import me.jayer.hdata.core.exception.HDataException
import tools.jackson.databind.JsonNode
import tools.jackson.databind.node.JsonNodeFactory
import tools.jackson.databind.node.ObjectNode

/**
 * Declaration of a single transform, aligned with Beam YAML's transform structure:
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
 * When [type] is `chain` or `composite`, this node is a composite transform, whose subgraph is
 * described by [transforms] / [source] / [sink] / [extraTransforms].
 *
 * @author wuya
 * @date 2022-08-30
 */
data class TransformSpec(
    /** When omitted, inferred as [COMPOSITE] based on whether there are child nodes; see [kind]. */
    val type: String = "",
    val name: String? = null,
    /** Single input: write `input: Foo`; multiple inputs: write `input: {A: Foo, B: Bar}` or `input: [Foo, Bar]`. */
    val input: JsonNode? = null,
    /** Alias of [input], identical semantics. */
    val inputs: JsonNode? = null,
    val config: JsonNode? = null,
    /** Child nodes of a composite transform. */
    val transforms: List<TransformSpec> = emptyList(),
    /** Shorthand for the first node of a composite transform. */
    val source: TransformSpec? = null,
    /** Shorthand for the last node of a composite transform. */
    val sink: TransformSpec? = null,
    /** Nodes appended outside the chain, which may reference any name inside the chain (typical use is to consume the error stream). */
    val extraTransforms: List<TransformSpec> = emptyList(),
    val windowing: WindowingSpec? = null,
    /** Output of a composite transform, a reference to some internal node. */
    val output: JsonNode? = null,
) {

    companion object {
        const val CHAIN = "chain"
        const val COMPOSITE = "composite"

        /** The placeholder key used when the input port is not explicitly named; the framework backfills it with the port name declared by the provider. */
        const val DEFAULT_INPUT_KEY = ""
    }

    /** The node name in the DAG; falls back to [type] when not explicitly declared. */
    val displayName: String get() = name ?: type.ifBlank { "<unnamed>" }

    /** The effective transform type: an explicit [type] takes precedence, defaulting to [COMPOSITE] when child nodes are present. */
    val kind: String
        get() = when {
            type.isNotBlank() -> type
            hasChildren() -> COMPOSITE
            else -> throw HDataException("transform is missing the type field: ${name?.let { "name=$it" } ?: this}")
        }

    val composite: Boolean get() = kind == CHAIN || kind == COMPOSITE

    val chain: Boolean get() = kind == CHAIN

    private fun hasChildren(): Boolean =
        source != null || sink != null || transforms.isNotEmpty() || extraTransforms.isNotEmpty()

    /** The child nodes of a composite node, expanded in source -> transforms -> sink order. */
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
            else -> throw HDataException("Transform[$displayName]'s config must be an object, but was: ${config.nodeType}")
        }

    /**
     * Resolves input references. The key is the input port name, and [DEFAULT_INPUT_KEY] means "let the provider decide the port name".
     * The value is a reference string of the form `TransformName` or `TransformName.outputTag`.
     */
    fun inputRefs(): Map<String, String> {
        if (input != null && inputs != null) {
            throw HDataException("Transform[$displayName] cannot declare both input and inputs")
        }
        val node = input ?: inputs ?: return emptyMap()
        return when {
            node.isNull -> emptyMap()
            node.isString -> mapOf(DEFAULT_INPUT_KEY to node.stringValue())
            node.isArray -> node.mapIndexed { index, element -> index.toString() to refOf(element) }.toMap()
            node.isObject -> node.properties().associate { (key, value) -> key to refOf(value) }
            else -> throw HDataException("Transform[$displayName]'s input must be a string, array or object, but was: ${node.nodeType}")
        }
    }

    /** The output references of a composite node; same semantics as [inputRefs]. */
    fun outputRefs(): Map<String, String> {
        val node = output ?: return emptyMap()
        return when {
            node.isNull -> emptyMap()
            node.isString -> mapOf(DEFAULT_INPUT_KEY to node.stringValue())
            node.isObject -> node.properties().associate { (key, value) -> key to refOf(value) }
            else -> throw HDataException("Transform[$displayName]'s output must be a string or object, but was: ${node.nodeType}")
        }
    }

    private fun refOf(node: JsonNode): String =
        if (node.isString) node.stringValue()
        else throw HDataException("Transform[$displayName]'s input/output reference must be a string, but was: ${node.nodeType}")
}
