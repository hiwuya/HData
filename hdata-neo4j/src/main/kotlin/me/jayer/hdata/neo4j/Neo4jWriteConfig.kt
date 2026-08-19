package me.jayer.hdata.neo4j

import me.jayer.hdata.neo4j.internal.extractCypherParams
import java.io.Serializable

/**
 * `WriteToNeo4j` 的配置。
 *
 * 写入就是执行一条 Cypher 语句，把行里的字段按名绑定成 `$param` 占位符：
 * - 显式用 `parameters` 给出 `cypher参数名 -> 行字段名` 的映射；
 * - 不填则自动从 `statement` 里提取 `$xxx`，按同名绑定到行字段。
 *
 * [batchSize] 行为一个事务一起提交；批量失败会退回逐条写以定位坏数据，
 * 见 [me.jayer.hdata.neo4j.transform.Neo4jWriteFn]。
 *
 * @author wuya
 */
data class Neo4jWriteConfig(
    override val uri: String = "bolt://localhost:7687",
    override val user: String = "neo4j",
    override val password: String = "",
    override val database: String? = null,
    val statement: String,
    val parameters: Map<String, String>? = null,
    val batchSize: Int = 1000,
) : Neo4jConnectionConfig {

    fun validate() {
        validateConnection()
        require(statement.isNotBlank()) { "statement 不能为空" }
        require(batchSize > 0) { "batch_size 必须 > 0" }
        val placeholders = extractCypherParams(statement)
        parameters?.let { mapping ->
            require(mapping.keys == placeholders) {
                "parameters 的键必须与 statement 占位符完全一致；占位符=$placeholders，映射键=${mapping.keys}"
            }
            require(mapping.values.none { it.isBlank() }) { "parameters 的行字段名不能为空" }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
