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
import org.neo4j.driver.Transaction
import org.neo4j.driver.Values
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 没有轻量的进程内 Neo4j 替身，这里用 Mockito 伪造 Driver/Session/Result，
 * 验证查询结果的行映射与写入时的参数绑定逻辑。
 */
class Neo4jPipelineTest {

    /**
     * 记录下每条真正执行到的语句与参数。
     *
     * 写入端是攒批 + 单事务提交的，所以语句走的是 `Transaction.run` 而不是 `Session.run`；
     * [failName] 用来模拟某一行写失败，验证批量失败之后退回逐条写的路径。
     *
     * 必须是 object 而不是带 lambda 字段的 class：DriverFactory 是 DoFn 的字段，
     * 要跟着 DoFn 一起序列化（捕获 Kotlin lambda 会让整个 DoFn 序列化不了），
     * 而且断言看的是 DirectRunner 进程内共享的这份静态状态，不是反序列化出来的副本。
     */
    private object CapturingFactory : DriverFactory {
        // DirectRunner 会把 bundle 分到多个线程上跑，这些计数器都会被并发更新，
        // 用普通的 ArrayList / Int 会丢记录（表现是断言数量时少一两条的偶发失败）
        val captured: MutableList<Pair<String, Map<String, Any?>>> =
            java.util.Collections.synchronizedList(mutableListOf())
        val commits = java.util.concurrent.atomic.AtomicInteger()
        val rollbacks = java.util.concurrent.atomic.AtomicInteger()

        /** 走事务的（攒批路径）与走 session 的（批量失败后逐条重来）分开计，好分辨走的是哪条路 */
        val txRuns = java.util.concurrent.atomic.AtomicInteger()
        val sessionRuns = java.util.concurrent.atomic.AtomicInteger()

        /** 参数里 `name` 等于它的那一行会写失败。 */
        var failName: String? = null

        fun reset(failName: String? = null) {
            captured.clear()
            commits.set(0)
            rollbacks.set(0)
            txRuns.set(0)
            sessionRuns.set(0)
            this.failName = failName
        }

        private fun record(statement: String, params: Map<String, Any?>): Result {
            if (failName != null && params["name"] == failName) {
                throw RuntimeException("写不进去: $params")
            }
            captured.add(statement to params)
            return mock<Result>()
        }

        override fun create(config: Neo4jConnectionConfig): Driver {
            val tx = mock<Transaction> {
                on { run(any<String>(), any<Map<String, Any>>()) } doAnswer { inv ->
                    txRuns.incrementAndGet()
                    record(inv.arguments[0] as String, inv.arguments[1] as Map<String, Any?>)
                }
                on { commit() } doAnswer { commits.incrementAndGet(); null }
                on { rollback() } doAnswer { rollbacks.incrementAndGet(); null }
            }
            val session = mock<Session> {
                on { beginTransaction() } doReturn tx
                on { run(any<String>(), any<Map<String, Any>>()) } doAnswer { inv ->
                    sessionRuns.incrementAndGet()
                    record(inv.arguments[0] as String, inv.arguments[1] as Map<String, Any?>)
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
        CapturingFactory.reset()
        val factory = CapturingFactory
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
                    driverFactory = factory,
                ),
            ),
        ).setRowSchema(schema)
        p.run()

        assertEquals(2, factory.captured.size)
        val snapshot = factory.captured.toList()
        val names = snapshot.map { it.second["name"] }.toSet()
        val ages = snapshot.map { it.second["age"] }.toSet()
        assertEquals(setOf("alice", "bob"), names)
        assertEquals(setOf(30L, 40L), ages)
        snapshot.forEach { (stmt, _) ->
            assertEquals("CREATE (n:Person {name: \$name, age: \$age})", stmt)
        }
    }

    @Test
    fun `攒批写入走单个事务，batch_size 不再是死参数`() {
        // batch_size 之前收下就丢掉：每行开一个 session 跑一条语句再关掉，
        // 每行一次网络往返加一次事务提交
        CapturingFactory.reset()
        val factory = CapturingFactory
        val config = Neo4jWriteConfig(statement = "CREATE (n:Person {name: \$name})", batchSize = 10)
        val schema = Schema.builder().addStringField("name").build()
        val rows = (1..5).map { Row.withSchema(schema).addValue("p$it").build() }

        val p = Pipeline.create()
        p.apply(Create.of(rows).withRowSchema(schema))
            .apply(ParDo.of(Neo4jWriteFn(config, ErrorSchemas.of(schema), false, "WriteToNeo4j", factory)))
            .setRowSchema(ErrorSchemas.of(schema))
        p.run().waitUntilFinish()

        assertEquals(5, factory.captured.size)
        // 关键是走了事务这条路：之前每行是各自开 session 跑一条 autocommit 语句。
        // 一个 bundle 提交一次事务，DirectRunner 怎么切 bundle 不影响这个断言
        assertEquals(5, factory.txRuns.get(), "所有行都该走事务批量提交")
        assertEquals(0, factory.sessionRuns.get(), "没有失败就不该退回逐条写")
        assertTrue(factory.commits.get() >= 1, "至少提交一次")
        assertEquals(0, factory.rollbacks.get())
    }

    @Test
    fun `批量失败后退回逐条写，只有坏的那行进死信`() {
        CapturingFactory.reset(failName = "bad")
        val factory = CapturingFactory
        val config = Neo4jWriteConfig(statement = "CREATE (n:Person {name: \$name})", batchSize = 10)
        val schema = Schema.builder().addStringField("name").build()
        val rows = listOf("good1", "bad", "good2").map { Row.withSchema(schema).addValue(it).build() }
        val errorSchema = ErrorSchemas.of(schema)

        val p = Pipeline.create()
        val errors = p.apply(Create.of(rows).withRowSchema(schema))
            .apply(ParDo.of(Neo4jWriteFn(config, errorSchema, true, "WriteToNeo4j", factory)))
            .setRowSchema(errorSchema)
        PAssert.that(errors).satisfies { output ->
            val list = output.toList()
            assertEquals(1, list.size, "只有坏的那一行该进死信")
            assertEquals("bad", list.single().getRow(ErrorSchemas.ELEMENT)!!.getString("name"))
            null
        }
        p.run().waitUntilFinish()

        assertTrue(factory.rollbacks.get() >= 1, "批量失败必须回滚")
        assertTrue(factory.sessionRuns.get() > 0, "批量失败之后要退回逐条写")
        // 逐条重来时两条好数据仍然写进去了
        assertEquals(setOf("good1", "good2"), factory.captured.map { it.second["name"] }.toSet())
    }
}
