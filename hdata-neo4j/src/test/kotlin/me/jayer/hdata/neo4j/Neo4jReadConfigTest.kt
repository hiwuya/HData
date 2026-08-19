package me.jayer.hdata.neo4j

import org.apache.beam.sdk.schemas.Schema
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class Neo4jReadConfigTest {

    @Test
    fun `query 与 schema_fields 必填`() {
        assertFailsWith<IllegalArgumentException> {
            Neo4jReadConfig(query = "", schemaFields = listOf("a:STRING")).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            Neo4jReadConfig(query = "MATCH (n) RETURN n", schemaFields = emptyList()).validate()
        }
    }

    @Test
    fun `schema_fields 解析为输出 schema`() {
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
    fun `未知字段类型报错`() {
        assertFailsWith<IllegalArgumentException> {
            Neo4jReadConfig(query = "RETURN 1", schemaFields = listOf("x:WEIRD")).validate()
        }
    }

    @Test
    fun `schema_fields 空字段名报错`() {
        // 字段名在构图阶段就解析，空字段名（":STRING"）会让输出 schema 出现空列名，必须当场报错
        assertFailsWith<IllegalArgumentException> {
            Neo4jReadConfig(query = "RETURN 1", schemaFields = listOf(" :STRING")).validate()
        }
    }

    @Test
    fun `连接配置与重复字段会校验`() {
        assertFailsWith<IllegalArgumentException> {
            Neo4jReadConfig(uri = "", query = "RETURN 1", schemaFields = listOf("x:INT64")).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            Neo4jReadConfig(query = "RETURN 1", schemaFields = listOf("x:INT64", "x:STRING")).validate()
        }
    }
}
