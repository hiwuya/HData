package me.jayer.hdata.mongodb

import com.mongodb.bulk.BulkWriteResult
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.InsertOneModel
import com.mongodb.client.model.WriteModel
import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.mongodb.transform.MongoWriteFn
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.Row
import org.bson.Document
import org.junit.jupiter.api.BeforeEach
import java.io.Serializable
import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.util.Collections
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * End-to-end test of `MongoWriteFn` running on a **full pipeline** (DirectRunner).
 *
 * Unit tests that only call `processElement` miss two kinds of problems: a non-serializable object slipped into the
 * DoFn (which only blows up when the job is submitted), and the batching/dead-letter behavior being wrong under Beam's
 * bundle lifecycle. This test covers both.
 *
 * The fake client uses `java.lang.reflect.Proxy` instead of Mockito: the MongoDB driver exposes only interfaces, and
 * as long as the `Proxy`'s `InvocationHandler` is serializable, the proxy itself **serializes and ships along with the
 * DoFn**; Mockito mocks are not serializable by default, so the injected fake client would silently vanish on the worker.
 *
 * @author wuya
 */
class MongoPipelineTest {

    /** All fake-client records land here: the deserialized handler is a different instance, so only static state can be aligned across JVM boundaries. */
    object MongoFakes {
        val batches: MutableList<List<Document>> = Collections.synchronizedList(mutableListOf())

        /** When non-null, the next `bulkWrite` throws it. */
        @Volatile
        var bulkError: Throwable? = null

        fun reset() {
            batches.clear()
            bulkError = null
        }
    }

    private object CollectionHandler : InvocationHandler, Serializable {
        override fun invoke(proxy: Any, method: Method, args: Array<Any?>?): Any? = when (method.name) {
            "bulkWrite" -> {
                @Suppress("UNCHECKED_CAST")
                val models = args!![0] as List<WriteModel<Document>>
                MongoFakes.batches.add(models.map { (it as InsertOneModel<Document>).document })
                MongoFakes.bulkError?.let { throw it } ?: BulkWriteResult.unacknowledged()
            }
            "hashCode" -> System.identityHashCode(proxy)
            "equals" -> proxy === args?.get(0)
            "toString" -> "fake-collection"
            else -> null
        }
    }

    private object DatabaseHandler : InvocationHandler, Serializable {
        override fun invoke(proxy: Any, method: Method, args: Array<Any?>?): Any? = when (method.name) {
            "getCollection" -> proxyOf(MongoCollection::class.java, CollectionHandler)
            "hashCode" -> System.identityHashCode(proxy)
            "equals" -> proxy === args?.get(0)
            "toString" -> "fake-database"
            else -> null
        }
    }

    private object ClientHandler : InvocationHandler, Serializable {
        override fun invoke(proxy: Any, method: Method, args: Array<Any?>?): Any? = when (method.name) {
            "getDatabase" -> proxyOf(MongoDatabase::class.java, DatabaseHandler)
            "hashCode" -> System.identityHashCode(proxy)
            "equals" -> proxy === args?.get(0)
            "toString" -> "fake-client"
            else -> null
        }
    }

    private val codec = MongoRowCodec.of(listOf("id:STRING", "amount:DOUBLE"))
    private val errorSchema = ErrorSchemas.of(codec.schema)

    @BeforeEach
    fun setUp() = MongoFakes.reset()

    private fun row(id: String, amount: Double = 1.0): Row =
        Row.withSchema(codec.schema).addValue(id).addValue(amount).build()

    private fun writeFn(batchSize: Int = 2): MongoWriteFn {
        val fn = MongoWriteFn(
            MongoWriteConfig(
                "mongodb://localhost:27017", "db", "c",
                listOf("id:STRING", "amount:DOUBLE"),
                batchSize = batchSize,
            ),
            codec,
            errorSchema,
            true,
            "WriteToMongoDb",
        )
        fn.testClient = proxyOf(MongoClient::class.java, ClientHandler)
        return fn
    }

    @Test
    fun `in a full pipeline a successful write produces no dead letter and hands every row to MongoDB`() {
        val pipeline = org.apache.beam.sdk.Pipeline.create()
        val input = pipeline.apply(Create.of(row("a1"), row("a2")).withRowSchema(codec.schema))

        val errors = input.apply(ParDo.of(writeFn(batchSize = 100))).setRowSchema(errorSchema)

        PAssert.that(errors).empty()
        pipeline.run().waitUntilFinish()

        // How DirectRunner splits bundles is its own business, so here we only check "both rows were handed off",
        // "only fire once batch_size is reached" is pinned down by MongoWriteBundleTest calling processElement directly
        assertEquals(
            listOf("a1", "a2"),
            MongoFakes.batches.flatten().map { it.getString("id") }.sortedBy { it },
        )
    }

    @Test
    fun `in a full pipeline failed rows go to dead letter carrying the original row`() {
        MongoFakes.bulkError = IllegalStateException("no reachable server")
        val pipeline = org.apache.beam.sdk.Pipeline.create()
        val input = pipeline.apply(Create.of(row("a1"), row("a2")).withRowSchema(codec.schema))

        val errors = input.apply(ParDo.of(writeFn(batchSize = 100))).setRowSchema(errorSchema)

        PAssert.that(errors).satisfies { rows ->
            val failures = rows.toList()
            assertEquals(2, failures.size, "Both rows should go to dead letter when the whole batch fails")
            val ids = failures.map { it.getValue<Row>(ErrorSchemas.ELEMENT).getString("id") }.sortedBy { it }
            assertEquals(listOf("a1", "a2"), ids, "Dead letter must contain the original rows; if lost it cannot be replayed")
            val message = failures[0].getString(ErrorSchemas.ERROR_MESSAGE)
            assertTrue(message!!.contains("no reachable server"), "Dead letter must carry the real exception, actual: $message")
            null
        }
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `a row missing a column declared in schema_fields only rejects that row instead of failing the job`() {
        val partial = Schema.builder().addNullableStringField("id").build()
        val bad = Row.withSchema(partial).addValue("a1").build()
        val pipeline = org.apache.beam.sdk.Pipeline.create()
        val input = pipeline.apply(Create.of(bad).withRowSchema(partial))

        // The input row is missing the amount column declared by the codec, so toModel rejects just this row to dead letter rather than failing the whole job
        val errors = input.apply(ParDo.of(writeFn(batchSize = 2)))
            .setRowSchema(ErrorSchemas.of(partial))

        PAssert.that(errors).satisfies { rows -> assertEquals(1, rows.toList().size); null }
        pipeline.run().waitUntilFinish()
        assertTrue(MongoFakes.batches.isEmpty(), "Rejected rows must not be sent to MongoDB")
    }


    private companion object {
        @Suppress("UNCHECKED_CAST")
        fun <T> proxyOf(iface: Class<T>, handler: InvocationHandler): T =
            Proxy.newProxyInstance(iface.classLoader, arrayOf(iface), handler) as T
    }
}
