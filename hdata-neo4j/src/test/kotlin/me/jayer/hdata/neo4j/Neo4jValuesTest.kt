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
    fun `从 Cypher 提取参数名`() {
        assertEquals(
            setOf("name", "age"),
            extractCypherParams("CREATE (n:Person {name: \$name, age: \$age})"),
        )
    }

    @Test
    fun `自动按同名绑定行字段`() {
        val schema = Schema.builder().addStringField("name").addInt64Field("age").build()
        val row = Row.withSchema(schema).addValue("alice").addValue(30L).build()
        val params = buildParams(row, "CREATE (n:Person {name: \$name, age: \$age})", null)
        assertEquals("alice", params["name"])
        assertEquals(30L, params["age"])
    }

    @Test
    fun `用显式映射绑定`() {
        val schema = Schema.builder().addStringField("n").addInt64Field("a").build()
        val row = Row.withSchema(schema).addValue("bob").addValue(40L).build()
        val params = buildParams(row, "CREATE (n:Person {name: \$x, age: \$y})", mapOf("x" to "n", "y" to "a"))
        assertEquals("bob", params["x"])
        assertEquals(40L, params["y"])
    }

    @Test
    fun `缺失行字段报错`() {
        val schema = Schema.builder().addStringField("name").build()
        val row = Row.withSchema(schema).addValue("alice").build()
        val e = runCatching { buildParams(row, "CREATE (n:Person {age: \$age})", null) }
        assertTrue(e.isFailure)
    }

    @Test
    fun `类型转换`() {
        assertEquals(30L, convertToRowValue(30L, Schema.FieldType.INT64))
        assertEquals(1, convertToRowValue(1, Schema.FieldType.INT32))
        assertEquals("x", convertToRowValue("x", Schema.FieldType.STRING))
        assertEquals(true, convertToRowValue(true, Schema.FieldType.BOOLEAN))
        assertNull(convertToRowValue(null, Schema.FieldType.STRING))
    }
}
