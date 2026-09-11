package me.jayer.hdata.neo4j

import org.apache.beam.sdk.schemas.Schema
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class Neo4jReadConfigTest {

    @Test
    fun `query and schema_fields are required`() {
        assertFailsWith<IllegalArgumentException> {
            Neo4jReadConfig(query = "", schemaFields = listOf("a:STRING")).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            Neo4jReadConfig(query = "MATCH (n) RETURN n", schemaFields = emptyList()).validate()
        }
    }

    @Test
    fun `schema_fields parses into the output schema`() {
        val config = Neo4jReadConfig(
            query = "RETURN 1",
            schemaFields = listOf("name:STRING", "age:INT64", "score:FLOAT64", "ok:BOOLEAN"),
        )
        val schema = config.outputSchema()
        assertEquals(Schema.TypeName.STRING, schema.getField("name").type.typeName)
        assertEquals(Schema.TypeName.INT64, schema.getField("age").type.typeName)
        assertEquals(Schema.TypeName.DOUBLE, schema.getField("score").type.typeName)
        assertEquals(Schema.TypeName.BOOLEAN, schema.getField("ok").type.typeName)
    }

    @Test
    fun `an unknown field type errors`() {
        assertFailsWith<IllegalArgumentException> {
            Neo4jReadConfig(query = "RETURN 1", schemaFields = listOf("x:WEIRD")).validate()
        }
    }

    @Test
    fun `an empty field name in schema_fields errors`() {
        // Field names are parsed at graph-construction time; an empty field name (":STRING") would produce a blank output
        // column name, which must be rejected right away
        assertFailsWith<IllegalArgumentException> {
            Neo4jReadConfig(query = "RETURN 1", schemaFields = listOf(" :STRING")).validate()
        }
    }

    @Test
    fun `connection config and duplicate fields are validated`() {
        assertFailsWith<IllegalArgumentException> {
            Neo4jReadConfig(uri = "", query = "RETURN 1", schemaFields = listOf("x:INT64")).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            Neo4jReadConfig(query = "RETURN 1", schemaFields = listOf("x:INT64", "x:STRING")).validate()
        }
    }
}
