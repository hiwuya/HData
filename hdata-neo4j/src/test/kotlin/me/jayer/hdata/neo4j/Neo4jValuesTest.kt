package me.jayer.hdata.neo4j

import me.jayer.hdata.neo4j.internal.buildParams
import me.jayer.hdata.neo4j.internal.convertToRowValue
import me.jayer.hdata.neo4j.internal.extractCypherParams
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class Neo4jValuesTest {

    @Test
    fun `extracts parameter names from Cypher`() {
        assertEquals(
            setOf("name", "age"),
            extractCypherParams("CREATE (n:Person {name: \$name, age: \$age})"),
        )
    }

    @Test
    fun `a dollar sign in a string literal, identifier, or comment is not a parameter`() {
        val dollar = '$'
        val statement = """
            CREATE (n {literal: '${dollar}not_param', quoted: "${dollar}also_not"})
            SET n.`${dollar}identifier` = ${dollar}real
            // ${dollar}line_comment
            /* ${dollar}block_comment */
            RETURN ${dollar}_second
        """.trimIndent()

        assertEquals(setOf("real", "_second"), extractCypherParams(statement))
    }

    @Test
    fun `auto-binds row fields of the same name`() {
        val schema = Schema.builder().addStringField("name").addInt64Field("age").build()
        val row = Row.withSchema(schema).addValue("alice").addValue(30L).build()
        val params = buildParams(row, "CREATE (n:Person {name: \$name, age: \$age})", null)
        assertEquals("alice", params["name"])
        assertEquals(30L, params["age"])
    }

    @Test
    fun `binds via an explicit mapping`() {
        val schema = Schema.builder().addStringField("n").addInt64Field("a").build()
        val row = Row.withSchema(schema).addValue("bob").addValue(40L).build()
        val params = buildParams(row, "CREATE (n:Person {name: \$x, age: \$y})", mapOf("x" to "n", "y" to "a"))
        assertEquals("bob", params["x"])
        assertEquals(40L, params["y"])
    }

    @Test
    fun `a missing row field errors`() {
        val schema = Schema.builder().addStringField("name").build()
        val row = Row.withSchema(schema).addValue("alice").build()
        val e = runCatching { buildParams(row, "CREATE (n:Person {age: \$age})", null) }
        assertTrue(e.isFailure)
    }

    @Test
    fun `type conversion`() {
        assertEquals(30L, convertToRowValue(30L, Schema.FieldType.INT64))
        assertEquals(1, convertToRowValue(1, Schema.FieldType.INT32))
        assertEquals("x", convertToRowValue("x", Schema.FieldType.STRING))
        assertEquals(true, convertToRowValue(true, Schema.FieldType.BOOLEAN))
        assertNull(convertToRowValue(null, Schema.FieldType.STRING))
    }
}
