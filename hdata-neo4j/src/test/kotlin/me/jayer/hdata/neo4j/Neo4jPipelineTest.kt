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
 * There is no lightweight in-process Neo4j stand-in, so this fakes Driver/Session/Result with Mockito to
 * verify the row mapping of query results and the parameter-binding logic on write.
 */
class Neo4jPipelineTest {

    /**
     * Records every statement and parameter set that actually got executed.
     *
     * The write side batches rows and commits them in a single transaction, so statements go through
     * `Transaction.run` rather than `Session.run`; [failName] simulates one row failing to write, to verify
     * the fallback path of retrying row by row after a batch failure.
     *
     * Must be an `object`, not a class with lambda fields: DriverFactory is a DoFn field and has to be
     * serialized along with the DoFn (capturing a Kotlin lambda would make the whole DoFn unserializable),
     * and the assertions inspect this static state shared within the DirectRunner process, not a
     * deserialized copy of it.
     */
    private object CapturingFactory : DriverFactory {
        // DirectRunner spreads bundles across multiple threads, so these counters are updated concurrently;
        // a plain ArrayList/Int would drop updates (showing up as an occasional off-by-one/two count assertion failure)
        val captured: MutableList<Pair<String, Map<String, Any?>>> =
            java.util.Collections.synchronizedList(mutableListOf())
        val commits = java.util.concurrent.atomic.AtomicInteger()
        val rollbacks = java.util.concurrent.atomic.AtomicInteger()

        /** Counted separately from the transaction path (batching) vs. the session path (retrying row by row after a batch failure), to tell which one actually ran. */
        val txRuns = java.util.concurrent.atomic.AtomicInteger()
        val sessionRuns = java.util.concurrent.atomic.AtomicInteger()

        /** The row whose `name` parameter equals this fails to write. */
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
                throw RuntimeException("failed to write: $params")
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
    fun `reading maps query results into rows`() {
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
    fun `writing binds row fields into Cypher parameters`() {
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
    fun `a batched write goes through a single transaction, batch_size is no longer a dead parameter`() {
        // batch_size used to be accepted and then discarded: every row opened its own session, ran one
        // statement, and closed it -- one network round trip plus one transaction commit per row
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
        // What matters is that it went through the transaction path: previously every row opened its own
        // session and ran one autocommit statement. One bundle commits one transaction; however DirectRunner
        // happens to split bundles does not affect this assertion
        assertEquals(5, factory.txRuns.get(), "every row should go through a batched transaction")
        assertEquals(0, factory.sessionRuns.get(), "with no failure, it should not fall back to writing row by row")
        assertTrue(factory.commits.get() >= 1, "should commit at least once")
        assertEquals(0, factory.rollbacks.get())
    }

    @Test
    fun `after a batch failure it falls back to writing row by row, only the bad row goes to the dead letter`() {
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
            assertEquals(1, list.size, "only the bad row should go to the dead letter")
            assertEquals("bad", list.single().getRow(ErrorSchemas.ELEMENT)!!.getString("name"))
            null
        }
        p.run().waitUntilFinish()

        assertTrue(factory.rollbacks.get() >= 1, "a batch failure must roll back")
        assertTrue(factory.sessionRuns.get() > 0, "after a batch failure it must fall back to writing row by row")
        // the two good rows still get written when retried row by row
        assertEquals(setOf("good1", "good2"), factory.captured.map { it.second["name"] }.toSet())
    }
}
