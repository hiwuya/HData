package me.jayer.hdata.core.graph

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.registry.TransformRegistry
import me.jayer.hdata.core.spec.ErrorHandlingSpec
import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spec.TransformSpec
import me.jayer.hdata.core.spec.WindowingSpec
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TransformProvider
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import tools.jackson.databind.node.ObjectNode

/**
 * Translates the graph described by [TransformSpec] into a Beam DAG.
 *
 * Before the refactor, `HData.start()` could only express "each source chained through all
 * transforms and then fed to all sinks" — it could build neither branches nor joins. This is
 * replaced by an explicit reference-based graph construction:
 *
 * - `chain`: linear, where the input is provided implicitly by the previous node;
 * - `composite`: an arbitrary DAG, where nodes declare their sources with `input: name` /
 *   `input: {A: x, B: y}`. The build order is decided by topological sort, so the write order
 *   does not matter;
 * - references support `name.outputPort`, and a dead-letter stream is consumed downstream via
 *   `name.<error_handling.output>`;
 * - composite nodes can be nested; each level is an independent namespace, and an inner level
 *   can look up names from the outer level.
 *
 * @author wuya
 * @date 2022-08-30
 */
class PipelineGraphBuilder(private val registry: TransformRegistry) {

    fun build(pipeline: Pipeline, root: TransformSpec): PipelineGraph {
        val session = Session(pipeline, registry)
        session.buildComposite(root, boundInput = null, parent = null, path = "")
        session.verifyErrorOutputsConsumed()
        return PipelineGraph(session.built)
    }

    private class Session(val pipeline: Pipeline, val registry: TransformRegistry) {
        val built = mutableListOf<GraphNode>()
        val scopes = mutableListOf<Scope>()

        fun buildComposite(
            spec: TransformSpec,
            boundInput: PCollectionRowTuple?,
            parent: Scope?,
            path: String,
        ): PCollectionRowTuple {
            val scope = Scope(parent, path).also { scopes.add(it) }
            if (boundInput != null) {
                scope.register(boundInputNode(boundInput))
            }

            val children = spec.children()
            val scopedChildren = children + spec.extraTransforms
            scopedChildren.forEach { child ->
                val name = child.displayName
                if (name.isBlank() || name == "<unnamed>") {
                    throw HDataException("${describe(path)}: a transform node must have a non-blank name; please declare type or name")
                }
                if ('.' in name) {
                    throw HDataException("${describe(path)}: node name [$name] must not contain '.', which is reserved for the node.outputPort reference syntax")
                }
                if (boundInput != null && name == BOUND_INPUT_NAME) {
                    throw HDataException("${describe(path)}: node name [input] conflicts with the composite node's reserved input name; please choose another name")
                }
            }
            scopedChildren.map { it.displayName }
                .groupingBy { it }
                .eachCount()
                .filterValues { it > 1 }
                .keys
                .forEach { duplicated ->
                    throw HDataException("${describe(path)}: multiple nodes share the name [$duplicated]; please distinguish them with name:")
                }

            val implicitInputs = implicitChainInputs(spec, children, boundInput != null)
            val order = if (spec.chain) children else topologicalOrder(children, path)
            for (child in order) {
                buildChild(scope, child, implicitInputs[child.displayName], spec.windowing, path)
            }
            for (extra in topologicalOrder(spec.extraTransforms, path)) {
                buildChild(scope, extra, null, spec.windowing, path)
            }

            return compositeOutput(spec, scope, children)
        }

        /** chain semantics: node i's input is node i-1's main output; the first node takes the composite node's input. */
        private fun implicitChainInputs(
            spec: TransformSpec,
            children: List<TransformSpec>,
            hasBoundInput: Boolean,
        ): Map<String, String> {
            if (!spec.chain) return emptyMap()
            return buildMap {
                children.forEachIndexed { index, child ->
                    val upstream = if (index == 0) {
                        if (hasBoundInput) BOUND_INPUT_NAME else null
                    } else {
                        children[index - 1].displayName
                    }
                    if (upstream != null) put(child.displayName, upstream)
                }
            }
        }

        private fun compositeOutput(
            spec: TransformSpec,
            scope: Scope,
            children: List<TransformSpec>,
        ): PCollectionRowTuple {
            val refs = spec.outputRefs()
            if (refs.isNotEmpty()) {
                var tuple = PCollectionRowTuple.empty(pipeline)
                for ((key, ref) in refs) {
                    val port = if (key == TransformSpec.DEFAULT_INPUT_KEY) Tags.MAIN_OUTPUT else key
                    tuple = tuple.and(port, scope.resolve(ref, spec.displayName))
                }
                return tuple
            }
            if (spec.chain) {
                val last = children.lastOrNull() ?: return PCollectionRowTuple.empty(pipeline)
                val node = scope.lookup(last.displayName)!!
                val main = node.mainOutput ?: return PCollectionRowTuple.empty(pipeline)
                return PCollectionRowTuple.of(Tags.MAIN_OUTPUT, node.outputs.getValue(main))
            }
            return PCollectionRowTuple.empty(pipeline)
        }

        private fun buildChild(
            scope: Scope,
            spec: TransformSpec,
            implicitInput: String?,
            inheritedWindowing: WindowingSpec?,
            path: String,
        ) {
            val name = spec.displayName
            val node = if (spec.composite) {
                buildNestedComposite(scope, spec, implicitInput, path, name)
            } else {
                buildLeaf(scope, spec, implicitInput, inheritedWindowing, path, name)
            }
            scope.register(node)
            built.add(node)
        }

        private fun buildNestedComposite(
            scope: Scope,
            spec: TransformSpec,
            implicitInput: String?,
            path: String,
            name: String,
        ): GraphNode {
            val refs = effectiveRefs(spec, implicitInput)
            val boundInput = if (refs.isEmpty()) null else {
                var tuple = PCollectionRowTuple.empty(pipeline)
                for ((key, ref) in refs) {
                    val port = if (key == TransformSpec.DEFAULT_INPUT_KEY) Tags.MAIN_INPUT else key
                    tuple = tuple.and(port, scope.resolve(ref, name))
                }
                tuple
            }
            val outputs = buildComposite(spec, boundInput, scope, childPath(path, name)).all
            return GraphNode(
                name = name,
                type = spec.kind,
                inputs = refs,
                outputs = outputs,
                mainOutput = mainOutputOf(outputs),
                errorAlias = null,
            )
        }

        private fun buildLeaf(
            scope: Scope,
            spec: TransformSpec,
            implicitInput: String?,
            inheritedWindowing: WindowingSpec?,
            path: String,
            name: String,
        ): GraphNode {
            val provider = registry.get(spec.kind)
            val config = spec.configNode().deepCopy()
            val errorHandling = extractErrorHandling(config, name)
            val refs = effectiveRefs(spec, implicitInput)

            var input = resolveInputs(scope, provider, spec, refs, name)
            val isRoot = input.all.isEmpty()
            // A transform-level window applies to its input; for a root node without input, it applies to its output
            if (spec.windowing != null && !isRoot) {
                input = applyWindowing(input, spec.windowing, "$name/Window")
            }

            val transform = provider.from(TransformConfig(name, config, errorHandling))
            var outputs = input.apply(childPath(path, name), transform)

            val rootWindowing = spec.windowing ?: inheritedWindowing
            if (isRoot && rootWindowing != null) {
                outputs = applyWindowing(outputs, rootWindowing, "$name/Window")
            }

            val outputMap = outputs.all
            if (errorHandling != null && !outputMap.containsKey(Tags.ERROR_OUTPUT)) {
                throw HDataException(
                    "transform[$name] of type [${spec.kind}] does not support error_handling; it does not produce a \"${Tags.ERROR_OUTPUT}\" output"
                )
            }
            if (errorHandling != null &&
                errorHandling.output != Tags.ERROR_OUTPUT &&
                outputMap.containsKey(errorHandling.output)
            ) {
                throw HDataException(
                    "transform[$name]'s error_handling.output[${errorHandling.output}] collides with one of the node's normal outputs"
                )
            }
            return GraphNode(
                name = name,
                type = spec.kind,
                inputs = if (provider.inputCollectionNames().isEmpty()) emptyMap() else refs,
                outputs = outputMap,
                mainOutput = mainOutputOf(outputMap),
                errorAlias = errorHandling?.output,
            )
        }

        private fun effectiveRefs(spec: TransformSpec, implicitInput: String?): Map<String, String> {
            val explicit = spec.inputRefs()
            if (explicit.isNotEmpty()) return explicit
            return if (implicitInput == null) emptyMap() else mapOf(TransformSpec.DEFAULT_INPUT_KEY to implicitInput)
        }

        private fun resolveInputs(
            scope: Scope,
            provider: TransformProvider,
            spec: TransformSpec,
            refs: Map<String, String>,
            name: String,
        ): PCollectionRowTuple {
            val type = spec.kind
            val declared = provider.inputCollectionNames()
            if (declared.isEmpty()) {
                // A source in a chain is implicitly wired with an input, which is normal; only an explicit declaration is an error
                if (spec.inputRefs().isNotEmpty()) {
                    throw HDataException("transform[$name] of type [$type] is a source and accepts no input, but ${refs.values} was declared")
                }
                return PCollectionRowTuple.empty(pipeline)
            }
            val variadic = declared.contains(Tags.ANY)
            if (refs.isEmpty()) {
                throw HDataException("transform[$name] of type [$type] requires an input; please declare its source with input:")
            }
            var tuple = PCollectionRowTuple.empty(pipeline)
            for ((key, ref) in refs) {
                val port = when {
                    key != TransformSpec.DEFAULT_INPUT_KEY -> key
                    variadic -> Tags.MAIN_INPUT
                    declared.size == 1 -> declared.single()
                    else -> throw HDataException(
                        "transform[$name] of type [$type] has multiple input ports $declared; please write input: {portName: source}"
                    )
                }
                if (!variadic && port !in declared) {
                    throw HDataException("transform[$name] has no input port [$port]; available ports: $declared")
                }
                tuple = tuple.and(port, scope.resolve(ref, name))
            }
            if (!variadic) {
                val missing = declared - tuple.all.keys
                if (missing.isNotEmpty()) {
                    throw HDataException("transform[$name] is missing input ports $missing")
                }
            }
            return tuple
        }

        private fun applyWindowing(
            tuple: PCollectionRowTuple,
            windowing: WindowingSpec,
            name: String,
        ): PCollectionRowTuple {
            val windowFn = windowing.toWindowFn()
            var windowed = PCollectionRowTuple.empty(pipeline)
            for ((port, collection) in tuple.all) {
                val schema = if (collection.hasSchema()) collection.schema else null
                val result = collection.apply("$name/$port", Window.into<Row>(windowFn))
                if (schema != null && !result.hasSchema()) {
                    result.setRowSchema(schema)
                }
                windowed = windowed.and(port, result)
            }
            return windowed
        }

        private fun extractErrorHandling(config: ObjectNode, name: String): ErrorHandlingSpec? {
            val node = config.remove(ErrorHandlingSpec.CONFIG_KEY) ?: return null
            val spec = try {
                SpecMappers.CONFIG.treeToValue(node, ErrorHandlingSpec::class.java)
            } catch (e: Exception) {
                throw HDataException("transform[$name]'s error_handling configuration is invalid: ${e.message}", e)
            }
            if (spec.threshold != null) {
                throw HDataException("transform[$name]'s error_handling.threshold is not implemented yet; please remove this field for now")
            }
            if (spec.output.isBlank()) {
                throw HDataException("transform[$name]'s error_handling.output must not be blank")
            }
            return spec
        }

        private fun topologicalOrder(children: List<TransformSpec>, path: String): List<TransformSpec> {
            if (children.size <= 1) return children
            val byName = children.associateBy { it.displayName }
            val dependencies = children.associate { child ->
                child.displayName to child.inputRefs().values
                    .map { it.substringBefore('.') }
                    .filter { byName.containsKey(it) }
                    .toSet()
            }
            val ordered = mutableListOf<TransformSpec>()
            val done = mutableSetOf<String>()
            val visiting = linkedSetOf<String>()

            fun visit(name: String) {
                if (name in done) return
                if (!visiting.add(name)) {
                    val cycle = (visiting.dropWhile { it != name } + name).joinToString(" -> ")
                    throw HDataException("${describe(path)} contains a cycle: $cycle")
                }
                dependencies.getValue(name).forEach(::visit)
                visiting.remove(name)
                done.add(name)
                ordered.add(byName.getValue(name))
            }

            children.forEach { visit(it.displayName) }
            return ordered
        }

        fun verifyErrorOutputsConsumed() {
            val unconsumed = scopes.flatMap { scope ->
                scope.nodes.values.filter { node ->
                    node.errorAlias != null && !scope.isConsumed(node.name, node.errorAlias)
                }
            }
            if (unconsumed.isNotEmpty()) {
                throw HDataException(
                    "The following transforms declared error_handling but their error streams are not consumed; " +
                        "add a downstream node or remove the declaration: " +
                        unconsumed.joinToString(", ") { "${it.name}.${it.errorAlias}" }
                )
            }
        }

        private fun boundInputNode(boundInput: PCollectionRowTuple): GraphNode {
            val outputs = boundInput.all
            return GraphNode(
                name = BOUND_INPUT_NAME,
                type = "<bound>",
                inputs = emptyMap(),
                outputs = outputs,
                mainOutput = mainOutputOf(outputs),
                errorAlias = null,
            )
        }

        private fun describe(path: String): String = if (path.isEmpty()) "pipeline" else "composite[$path]"

        private fun childPath(path: String, name: String): String = if (path.isEmpty()) name else "$path/$name"

        companion object {
            /** The name used when a composite node's inner nodes reference the composite node's own input. */
            const val BOUND_INPUT_NAME = "input"

            fun mainOutputOf(outputs: Map<String, PCollection<Row>>): String? = when {
                outputs.containsKey(Tags.MAIN_OUTPUT) -> Tags.MAIN_OUTPUT
                else -> outputs.keys.firstOrNull { it != Tags.ERROR_OUTPUT }
            }
        }
    }

        /** The namespace corresponding to one level of composite node. */
        private class Scope(val parent: Scope?, val path: String) {
        val nodes = linkedMapOf<String, GraphNode>()
        private val consumed = mutableSetOf<String>()

        fun register(node: GraphNode) {
            if (nodes.put(node.name, node) != null) {
                throw HDataException("Duplicate node name: ${node.name}${if (path.isEmpty()) "" else " (scope $path)"}")
            }
        }

        fun lookup(name: String): GraphNode? = nodes[name] ?: parent?.lookup(name)

        fun isConsumed(name: String, alias: String): Boolean = "$name.$alias" in consumed

        fun resolve(ref: String, requester: String): PCollection<Row> {
            val name = ref.substringBefore('.')
            val tag = ref.substringAfter('.', "").ifEmpty { null }
            val owner = ownerOf(name)
                ?: throw HDataException(
                    "transform[$requester] references a non-existent node [$name]; available nodes in scope: ${visibleNames()}"
                )
            val node = owner.nodes.getValue(name)
            if (tag == null) {
                val main = node.mainOutput
                    ?: throw HDataException("transform[$requester] references node [$name] which has no main output; available outputs: ${node.outputs.keys}")
                return node.outputs.getValue(main)
            }
            if (tag == node.errorAlias) {
                owner.consumed.add("$name.$tag")
                return node.outputs.getValue(Tags.ERROR_OUTPUT)
            }
            return node.outputs[tag]
                ?: throw HDataException("transform[$requester] references $name.$tag which does not exist; node [$name] outputs: ${node.outputs.keys}")
        }

        private fun ownerOf(name: String): Scope? = if (nodes.containsKey(name)) this else parent?.ownerOf(name)

        private fun visibleNames(): List<String> =
            (nodes.keys.toList() + (parent?.visibleNames() ?: emptyList())).distinct()
    }
}
