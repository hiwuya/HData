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
 * 把 [TransformSpec] 描述的图翻译成 Beam DAG。
 *
 * 重构前的 `HData.start()` 只能表达"每个 source 串上全部 transform 再喂给全部 sink"，
 * 既构不出分支也构不出 join。这里改成显式的引用式建图：
 *
 * - `chain`：线性，输入由上一个节点隐式提供；
 * - `composite`：任意 DAG，节点用 `input: 名字` / `input: {A: x, B: y}` 声明来源，
 *   构建顺序由拓扑排序决定，因此书写顺序无关；
 * - 引用支持 `名字.输出端口`，死信流即通过 `名字.<error_handling.output>` 被下游消费；
 * - 复合节点可以嵌套，每层是一个独立命名空间，内层可以向外层查名。
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
            children.map { it.displayName }
                .groupingBy { it }
                .eachCount()
                .filterValues { it > 1 }
                .keys
                .forEach { duplicated ->
                    throw HDataException("${describe(path)} 内有多个同名节点[$duplicated]，请用 name: 显式区分")
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

        /** chain 语义：第 i 个节点的输入是第 i-1 个节点的主输出，首节点吃复合节点的输入。 */
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
            // transform 级窗口作用于它的输入；根节点没有输入，则作用于它的输出
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
                    "transform[$name] 的类型[${spec.kind}] 不支持 error_handling，它没有产出 \"${Tags.ERROR_OUTPUT}\" 输出"
                )
            }
            if (errorHandling != null &&
                errorHandling.output != Tags.ERROR_OUTPUT &&
                outputMap.containsKey(errorHandling.output)
            ) {
                throw HDataException(
                    "transform[$name] 的 error_handling.output[${errorHandling.output}] 与该节点的普通输出同名"
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
                // chain 里的读取端会被隐式串上一个输入，这属于正常情况，只有显式声明才算写错
                if (spec.inputRefs().isNotEmpty()) {
                    throw HDataException("transform[$name] 的类型[$type] 是读取端，不接受输入，实际声明了: ${refs.values}")
                }
                return PCollectionRowTuple.empty(pipeline)
            }
            val variadic = declared.contains(Tags.ANY)
            if (refs.isEmpty()) {
                throw HDataException("transform[$name] 的类型[$type] 需要输入，请用 input: 声明来源")
            }
            var tuple = PCollectionRowTuple.empty(pipeline)
            for ((key, ref) in refs) {
                val port = when {
                    key != TransformSpec.DEFAULT_INPUT_KEY -> key
                    variadic -> Tags.MAIN_INPUT
                    declared.size == 1 -> declared.single()
                    else -> throw HDataException(
                        "transform[$name] 的类型[$type] 有多个输入端口 $declared，请写成 input: {端口名: 来源}"
                    )
                }
                if (!variadic && port !in declared) {
                    throw HDataException("transform[$name] 没有输入端口[$port]，可用端口: $declared")
                }
                tuple = tuple.and(port, scope.resolve(ref, name))
            }
            if (!variadic) {
                val missing = declared - tuple.all.keys
                if (missing.isNotEmpty()) {
                    throw HDataException("transform[$name] 缺少输入端口 $missing")
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
                throw HDataException("transform[$name] 的 error_handling 配置无效: ${e.message}", e)
            }
            if (spec.threshold != null) {
                throw HDataException("transform[$name] 的 error_handling.threshold 暂未实现，请先移除该字段")
            }
            if (spec.output.isBlank()) {
                throw HDataException("transform[$name] 的 error_handling.output 不能为空")
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
                    throw HDataException("${describe(path)} 内存在环: $cycle")
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
                    "以下 transform 声明了 error_handling 但错误流没有被消费，请加一个下游节点或删除声明: " +
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
            /** 复合节点内部引用"复合节点自身输入"时使用的名字。 */
            const val BOUND_INPUT_NAME = "input"

            fun mainOutputOf(outputs: Map<String, PCollection<Row>>): String? = when {
                outputs.containsKey(Tags.MAIN_OUTPUT) -> Tags.MAIN_OUTPUT
                else -> outputs.keys.firstOrNull { it != Tags.ERROR_OUTPUT }
            }
        }
    }

    /** 一层复合节点对应的命名空间。 */
    private class Scope(val parent: Scope?, val path: String) {
        val nodes = linkedMapOf<String, GraphNode>()
        private val consumed = mutableSetOf<String>()

        fun register(node: GraphNode) {
            if (nodes.put(node.name, node) != null) {
                throw HDataException("节点名重复: ${node.name}${if (path.isEmpty()) "" else "（作用域 $path）"}")
            }
        }

        fun lookup(name: String): GraphNode? = nodes[name] ?: parent?.lookup(name)

        fun isConsumed(name: String, alias: String): Boolean = "$name.$alias" in consumed

        fun resolve(ref: String, requester: String): PCollection<Row> {
            val name = ref.substringBefore('.')
            val tag = ref.substringAfter('.', "").ifEmpty { null }
            val owner = ownerOf(name)
                ?: throw HDataException(
                    "transform[$requester] 引用了不存在的节点[$name]，当前作用域可用节点: ${visibleNames()}"
                )
            val node = owner.nodes.getValue(name)
            if (tag == null) {
                val main = node.mainOutput
                    ?: throw HDataException("transform[$requester] 引用的节点[$name] 没有主输出，可用输出: ${node.outputs.keys}")
                return node.outputs.getValue(main)
            }
            if (tag == node.errorAlias) {
                owner.consumed.add("$name.$tag")
                return node.outputs.getValue(Tags.ERROR_OUTPUT)
            }
            return node.outputs[tag]
                ?: throw HDataException("transform[$requester] 引用的 $name.$tag 不存在，节点[$name] 的输出有: ${node.outputs.keys}")
        }

        private fun ownerOf(name: String): Scope? = if (nodes.containsKey(name)) this else parent?.ownerOf(name)

        private fun visibleNames(): List<String> =
            (nodes.keys.toList() + (parent?.visibleNames() ?: emptyList())).distinct()
    }
}
