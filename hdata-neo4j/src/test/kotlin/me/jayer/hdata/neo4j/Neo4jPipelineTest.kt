package me.jayer.hdata.neo4j

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.neo4j.internal.DriverFactory
import me.jayer.hdata.neo4j.internal.parseSchemaFields
import me.jayer.hdata.neo4j.transform.Neo4jReadFn
import me.jayer.hdata.neo4j.transform.Neo4jWriteFn
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.Row
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.neo4j.driver.Driver
import org.neo4j.driver.Record
import org.neo4j.driver.Result
import org.neo4j.driver.Session
import org.neo4j.driver.Values
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 没有轻量的进程内 Neo4j 替身，这里用 Mockito 伪造 Driver/Session/Result，
 * 验证查询结果的行映射与写入时的参数绑定逻辑。
 */
class Neo4jPipelineTest {

    private object CapturingFactory : DriverFactory {
        val captured = mutableListOf<Pair<String, Map<String, Any?>>>()
        override fun create(config: Neo4jConnectionConfig): Driver {
            val session = mock<Session> {
                on { run(any<String>(), any<Map<String, Any>>()) } doAnswer { inv ->
                    captured.add(inv.arguments[0] as String to (inv.arguments[1] as Map<String, Any?>))
                    mock<Result>()
                }
            }
            return mock { on { session() } doReturn session }
        }
    }

    private object ReadFactory : DriverFactory {
        override fun create(config: Neo4jConnectionConfig): Driver {
            val record1 = mock<Record> {
                on { get("name") } doReturn Values.value("alice")
                on { get("age") } doReturn Values.value(30L)
            }
            val record2 = mock<Record> {
                on { get("name") } doReturn Values.value("bob")
                on { get("age") } doReturn Values.value(40L)
            }
            val result = mock<Result> {
                on { hasNext() } doReturn true doReturn true doReturn false
                on { next() } doReturn record1 doReturn record2
            }
            val session = mock<Session> { on { run(any<String>(), any<Map<String, Any>>()) } doReturn result }
            return mock { on { session() } doReturn session }
        }
    }

    @Test
    fun `读取把查询结果映射成行`() {
        val factory = ReadFactory

        val config = Neo4jReadConfig(
            query = "MATCH (n) RETURN n.name AS name, n.age AS age",
            schemaFields = listOf("name:STRING", "age:INT64"),
        )
        val schema = config.outputSchema()
        val schemaFields = parseSchemaFields(config.schemaFields)

        val p = Pipeline.create()
        val trigger = p.apply(Create.of(listOf("")))
        val out = trigger.apply(ParDo.of(Neo4jReadFn(config, schema, schemaFields, factory))).setRowSchema(schema)
        PAssert.that(out).containsInAnyOrder(
            Row.withSchema(schema).addValue("alice").addValue(30L).build(),
            Row.withSchema(schema).addValue("bob").addValue(40L).build(),
        )
        p.run()
    }

    @Test
    fun `写入把行字段绑定成 Cypher 参数`() {
        CapturingFactory.captured.clear()
        val config = Neo4jWriteConfig(statement = "CREATE (n:Person {name: \$name, age: \$age})")
        val schema = Schema.builder().addStringField("name").addInt64Field("age").build()
        val rows = listOf(
            Row.withSchema(schema).addValue("alice").addValue(30L).build(),
            Row.withSchema(schema).addValue("bob").addValue(40L).build(),
        )
        val p = Pipeline.create()
        val input = p.apply(Create.of(rows).withRowSchema(schema))
        input.apply(
            ParDo.of(
                Neo4jWriteFn(
                    config,
                    ErrorSchemas.of(schema),
                    deadLetter = false,
                    transformName = "WriteToNeo4j",
                    driverFactory = CapturingFactory,
                ),
            ),
        ).setRowSchema(schema)
        p.run()

        assertEquals(2, CapturingFactory.captured.size)
        val names = CapturingFactory.captured.map { it.second["name"] }.toSet()
        val ages = CapturingFactory.captured.map { it.second["age"] }.toSet()
        assertEquals(setOf("alice", "bob"), names)
        assertEquals(setOf(30L, 40L), ages)
        CapturingFactory.captured.forEach { (stmt, _) ->
            assertEquals("CREATE (n:Person {name: \$name, age: \$age})", stmt)
        }
    }
}
