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
 * `EsWriteFn` 跑在**完整 pipeline**（DirectRunner）上的端到端测试。
 *
 * ES 8 的客户端是类、不是接口，不能用 `java.lang.reflect.Proxy` 伪造；而 Mockito 的可序列化 mock
 * 又依赖 subclass mock maker，本模块为了给 final 的 `BulkResponse` 打桩已经全局换成了 inline mock maker
 * （它生成的 mock 类没有无参构造器，Java 序列化还原不了）。
 *
 * 所以这里直接**继承** `ElasticsearchClient` 并覆盖 `bulk(BulkRequest)`（这个方法不是 final）：
 * 伪造的对象是可序列化的普通类，传输层用一个同样可序列化的 `Proxy` 顶上，反正 `bulk` 被覆盖了用不到它。
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

    /** 传输层用不到（`bulk` 被覆盖了），但构造器要一个非空值，给个可序列化的 Proxy 顶上。 */
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

    /** 工厂是可序列化的，跟着 DoFn 到 worker 上再造假的客户端。 */
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

    private fun row(id: String, name: String = "张三"): Row =
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
    fun `完整 pipeline 里写成功不产生死信，每一行都交给了 ES`() {
        val pipeline = Pipeline.create()
        val input = pipeline.apply(Create.of(row("a1"), row("a2")).withRowSchema(schema))

        val errors = input.apply(ParDo.of(writeFn())).setRowSchema(errorSchema)

        PAssert.that(errors).empty()
        pipeline.run().waitUntilFinish()

        assertEquals(2, EsFakes.batches.sum(), "两行都应该被提交给 ES")
    }

    @Test
    fun `完整 pipeline 里整批失败时全部行进死信，且带着原始行`() {
        EsFakes.bulkError = IllegalStateException("cluster blocked")
        val pipeline = Pipeline.create()
        val input = pipeline.apply(Create.of(row("a1")).withRowSchema(schema))

        val errors = input.apply(ParDo.of(writeFn())).setRowSchema(errorSchema)

        PAssert.that(errors).satisfies { rows ->
            val failures = rows.toList()
            assertEquals(1, failures.size)
            assertEquals("a1", failures[0].getValue<Row>(ErrorSchemas.ELEMENT).getString("id"))
            val message = failures[0].getString(ErrorSchemas.ERROR_MESSAGE)
            assertTrue(message!!.contains("cluster blocked"), "死信要带真实异常，实际: $message")
            null
        }
        pipeline.run().waitUntilFinish()
    }
}
