package me.jayer.hdata.neo4j

import java.io.Serializable

/**
 * `WriteToNeo4j` 的配置。
 *
 * 写入就是执行一条 Cypher 语句，把行里的字段按名绑定成 `$param` 占位符：
 * - 显式用 `parameters` 给出 `cypher参数名 -> 行字段名` 的映射；
 * - 不填则自动从 `statement` 里提取 `$xxx`，按同名绑定到行字段。
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
        require(statement.isNotBlank()) { "statement 不能为空" }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
