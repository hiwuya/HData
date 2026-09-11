package me.jayer.hdata.neo4j

import me.jayer.hdata.neo4j.internal.parseSchemaFields
import org.apache.beam.sdk.schemas.Schema
import java.io.Serializable

/**
 * Shared Neo4j connection settings for read and write configurations.
 *
 * @author wuya
 */
interface Neo4jConnectionConfig : Serializable {
    val uri: String
    val user: String
    val password: String
    val database: String?

    fun validateConnection() {
        require(uri.isNotBlank()) { "uri must not be blank" }
        require(user.isNotBlank()) { "user must not be blank" }
        val databaseName = database
        require(databaseName == null || databaseName.isNotBlank()) { "database must not be an empty string" }
    }
}

/**
 * Configuration for `ReadFromNeo4j`.
 *
 * The read side builds without a database connection. `schema_fields` declares the output schema and `query`
 * result fields are mapped to Rows by name. Use `parameters` for constant query parameters.
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
        require(query.isNotBlank()) { "query must not be blank" }
        require(schemaFields.isNotEmpty()) { "schema_fields must not be empty" }
        val fields = parseSchemaFields(schemaFields)
        require(fields.map { it.first }.distinct().size == fields.size) { "schema_fields field names must not be duplicated" }
    }

    fun outputSchema(): Schema = Schema.builder().apply {
        parseSchemaFields(schemaFields).forEach { (name, type) -> addNullableField(name, type) }
    }.build()

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
