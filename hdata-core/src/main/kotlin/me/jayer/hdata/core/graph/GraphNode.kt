package me.jayer.hdata.core.graph

import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.Row

/**
 * 构图结果里的一个节点，主要用于 `--dryRun` 打印与错误定位。
 *
 * @author wuya
 * @date 2022-08-30
 */
class GraphNode(
    val name: String,
    val type: String,
    /** 输入端口 -> 引用串。 */
    val inputs: Map<String, String>,
    val outputs: Map<String, PCollection<Row>>,
    /** 不带 tag 的引用（`Foo`）指向的输出端口，写入端为 null。 */
    val mainOutput: String?,
    /** `error_handling.output` 声明的别名。 */
    val errorAlias: String?,
) {
    fun describe(): String {
        val from = if (inputs.isEmpty()) "-" else inputs.entries.joinToString(", ") { (port, ref) ->
            if (port.isBlank()) ref else "$port=$ref"
        }
        val to = if (outputs.isEmpty()) "-" else outputs.keys.joinToString(", ")
        return "$name [$type] inputs($from) outputs($to)"
    }
}

/**
 * 整张图，[nodes] 按实际构建顺序排列。
 */
class PipelineGraph(val nodes: List<GraphNode>) {
    fun describe(): String = nodes.joinToString("\n") { "  ${it.describe()}" }
}
