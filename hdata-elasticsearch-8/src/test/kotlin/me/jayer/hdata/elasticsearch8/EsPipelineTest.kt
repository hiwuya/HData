package me.jayer.hdata.elasticsearch8

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch.core.BulkRequest
import co.elastic.clients.elasticsearch.core.BulkResponse
import co.elastic.clients.transport.ElasticsearchTransport
import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.elasticsearch8.transform.EsWriteFn
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.Row
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
 * End-to-end test of `EsWriteFn` running on a **full pipeline** (DirectRunner).
 *
 * The ES 8 client is a class, not an interface, so it cannot be faked with `java.lang.reflect.Proxy`;
 * and Mockito's serializable mocks depend on the subclass mock maker, while this module already
 * switched globally to the inline mock maker to stub the final `BulkResponse` (the mock classes it
 * generates have no no-arg constructor, so Java serialization cannot restore them).
 *
 * So this **subclasses** `ElasticsearchClient` directly and overrides `bulk(BulkRequest)` (which is
 * not final): the fake object is a plain serializable class, with a likewise-serializable `Proxy`
 * standing in for the transport layer — it is never actually used since `bulk` is overridden.
 *
 * @author wuya
 */
class EsPipelineTest {

    object EsFakes {
        val batches: MutableList<Int> = Collections.synchronizedList(mutableListOf())

        @Volatile
        var bulkError: Throwable? = null

        fun reset() {
            batches.clear()
            bulkError = null
        }
    }

    /** Unused by the transport layer (`bulk` is overridden), but the constructor needs a non-null value, so a serializable Proxy stands in. */
    private object TransportHandler : InvocationHandler, Serializable {
        override fun invoke(proxy: Any, method: Method, args: Array<Any?>?): Any? = when (method.name) {
            "hashCode" -> System.identityHashCode(proxy)
            "equals" -> proxy === args?.get(0)
            "toString" -> "fake-transport"
            else -> null
        }
    }

    private class FakeEsClient(transport: ElasticsearchTransport) : ElasticsearchClient(transport) {
        override fun bulk(request: BulkRequest): BulkResponse {
            EsFakes.batches.add(request.operations().size)
            EsFakes.bulkError?.let { throw it as Exception }
            return BulkResponse.of { it.errors(false).took(0L).items(emptyList()) }
        }
    }

    /** The factory is serializable and travels with the DoFn to the worker, building the fake client there. */
    private object FakeEsClientFactory : EsClientFactory {
        override fun create(): ElasticsearchClient {
            @Suppress("UNCHECKED_CAST")
            val transport = Proxy.newProxyInstance(
                ElasticsearchTransport::class.java.classLoader,
                arrayOf(ElasticsearchTransport::class.java),
                TransportHandler,
            ) as ElasticsearchTransport
            return FakeEsClient(transport)
        }
    }

    private val schema = buildSchema(listOf("id:STRING", "name:STRING"))
    private val errorSchema = ErrorSchemas.of(schema)

    @BeforeEach
    fun setUp() = EsFakes.reset()

    private fun row(id: String, name: String = "Alice"): Row =
        Row.withSchema(schema).addValue(id).addValue(name).build()

    private fun writeFn(batchSize: Int = 100): EsWriteFn {
        val fn = EsWriteFn(
            EsWriteConfig(
                connectionUri = "http://localhost:9200",
                index = "orders",
                schemaFields = listOf("id:STRING", "name:STRING"),
                batchSize = batchSize,
            ),
            schema,
            errorSchema,
            true,
            "WriteToElasticsearch8",
        )
        fn.clientFactory = FakeEsClientFactory
        return fn
    }

    @Test
    fun `a successful write in the full pipeline produces no dead letters, and every row reaches ES`() {
        val pipeline = Pipeline.create()
        val input = pipeline.apply(Create.of(row("a1"), row("a2")).withRowSchema(schema))

        val errors = input.apply(ParDo.of(writeFn())).setRowSchema(errorSchema)

        PAssert.that(errors).empty()
        pipeline.run().waitUntilFinish()

        assertEquals(2, EsFakes.batches.sum(), "both rows should have been submitted to ES")
    }

    @Test
    fun `when a whole batch fails in the full pipeline, every row goes to the dead letter carrying the original row`() {
        EsFakes.bulkError = IllegalStateException("cluster blocked")
        val pipeline = Pipeline.create()
        val input = pipeline.apply(Create.of(row("a1")).withRowSchema(schema))

        val errors = input.apply(ParDo.of(writeFn())).setRowSchema(errorSchema)

        PAssert.that(errors).satisfies { rows ->
            val failures = rows.toList()
            assertEquals(1, failures.size)
            assertEquals("a1", failures[0].getValue<Row>(ErrorSchemas.ELEMENT).getString("id"))
            val message = failures[0].getString(ErrorSchemas.ERROR_MESSAGE)
            assertTrue(message!!.contains("cluster blocked"), "the dead letter should carry the real exception, actual: $message")
            null
        }
        pipeline.run().waitUntilFinish()
    }
}
