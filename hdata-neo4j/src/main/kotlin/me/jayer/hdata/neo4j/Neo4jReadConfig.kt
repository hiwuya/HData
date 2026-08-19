package me.jayer.hdata.neo4j

import me.jayer.hdata.neo4j.internal.parseSchemaFields
import org.apache.beam.sdk.schemas.Schema
import java.io.Serializable

/**
 * Neo4j 连接信息，读/写配置共享。
 *
 * @author wuya
 */
interface Neo4jConnectionConfig : Serializable {
    val uri: String
    val user: String
    val password: String
    val database: String?

    fun validateConnection() {
        require(uri.isNotBlank()) { "uri 不能为空" }
        require(user.isNotBlank()) { "user 不能为空" }
        val databaseName = database
        require(databaseName == null || databaseName.isNotBlank()) { "database 不能为空字符串" }
    }
}

/**
 * `ReadFromNeo4j` 的配置。
 *
 * 读端**不连库即可构图**：输出 schema 由 `schema_fields`（`name:TYPE` 列表）声明，
 * 查询用 `query`（Cypher）返回的记录按这些字段名取出，逐行映射成 Row。
 * 常量查询参数用 `parameters` 注入。
 *
 * @author wuya
 */
data class Neo4jReadConfig(
    override val uri: String = "bolt://localhost:7687",
    override val user: String = "neo4j",
    override val password: String = "",
    override val database: String? = null,
    val query: String,
    val parameters: Map<String, Any>? = null,
    val schemaFields: List<String>,
) : Neo4jConnectionConfig {

    fun validate() {
        validateConnection()
        require(query.isNotBlank()) { "query 不能为空" }
        require(schemaFields.isNotEmpty()) { "schema_fields 不能为空" }
        val fields = parseSchemaFields(schemaFields)
        require(fields.map { it.first }.distinct().size == fields.size) { "schema_fields 字段名不能重复" }
    }

    fun outputSchema(): Schema = Schema.builder().apply {
        parseSchemaFields(schemaFields).forEach { (name, type) -> addNullableField(name, type) }
    }.build()

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
