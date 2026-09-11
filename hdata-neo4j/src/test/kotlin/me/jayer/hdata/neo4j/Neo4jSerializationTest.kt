package me.jayer.hdata.neo4j

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.neo4j.internal.RealDriverFactory
import me.jayer.hdata.neo4j.internal.parseSchemaFields
import me.jayer.hdata.neo4j.transform.Neo4jReadFn
import me.jayer.hdata.neo4j.transform.Neo4jWriteFn
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.util.SerializableUtils
import kotlin.test.Test

class Neo4jSerializationTest {

    @Test
    fun `DoFns are serializable`() {
        val readConfig = Neo4jReadConfig(query = "RETURN 1", schemaFields = listOf("a:STRING"))
        val schema = readConfig.outputSchema()
        val schemaFields = parseSchemaFields(readConfig.schemaFields)
        SerializableUtils.ensureSerializable(Neo4jReadFn(readConfig, schema, schemaFields, RealDriverFactory))

        val writeConfig = Neo4jWriteConfig(statement = "CREATE (n)")
        SerializableUtils.ensureSerializable(
            Neo4jWriteFn(writeConfig, ErrorSchemas.of(schema), deadLetter = true, transformName = "WriteToNeo4j", driverFactory = RealDriverFactory),
        )
    }
}
