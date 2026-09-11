package me.jayer.hdata.neo4j

import me.jayer.hdata.neo4j.internal.extractCypherParams
import java.io.Serializable

/**
 * Configuration for `WriteToNeo4j`.
 *
 * Executes Cypher, binding row fields to `$param` placeholders. `parameters` can map parameter names to row fields;
 * otherwise placeholders are bound by matching names. [batchSize] rows share one transaction.
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
        require(statement.isNotBlank()) { "statement must not be blank" }
        require(batchSize > 0) { "batch_size must be > 0" }
        val placeholders = extractCypherParams(statement)
        parameters?.let { mapping ->
            require(mapping.keys == placeholders) {
                "parameters keys must exactly match statement placeholders; placeholders=$placeholders, keys=${mapping.keys}"
            }
            require(mapping.values.none { it.isBlank() }) { "parameters row field names must not be blank" }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
