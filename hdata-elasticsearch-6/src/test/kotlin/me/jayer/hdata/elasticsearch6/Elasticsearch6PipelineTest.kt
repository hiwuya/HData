package me.jayer.hdata.elasticsearch6

import me.jayer.hdata.core.error.ErrorSchemas
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.Row
import org.elasticsearch.action.bulk.BulkItemResponse
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestHighLevelClient
import org.junit.jupiter.api.BeforeEach
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.io.Serializable
import java.util.Collections
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * `Elasticsearch6WriteFn` 跑在**完整 pipeline**（DirectRunner）上的端到端测试。
 *
 * `RestHighLevelClient` 的 `bulk` 是 final 方法，只能靠 inline mock maker 打桩，
 * 而它造出来的 mock 没法被 Java 序列化——所以假客户端由**可序列化的工厂在 worker 里现造**，
 * 而不是在测试 JVM 里造好塞给 DoFn。
 *
 * @author wuya
 */
class Elasticsearch6PipelineTest {

    object Es6Fakes {
        val batches: MutableList<Int> = Collections.synchronizedList(mutableListOf())

        @Volatile
        var bulkError: Throwable? = null

        fun reset() {
            batches.clear()
            bulkError = null
        }
    }

    /** 工厂可序列化，跟着 DoFn 到 worker 上再造假的客户端。 */
    private object FakeClientFactory : Es6ClientFactory, Serializable {
        override fun create(): RestHighLevelClient {
            val client = mock<RestHighLevelClient>()
            whenever(client.bulk(anyOrNull<BulkRequest>(), anyOrNull<RequestOptions>())).thenAnswer { inv ->
                Es6Fakes.batches.add(inv.getArgument<BulkRequest>(0).numberOfActions())
                Es6Fakes.bulkError?.let { throw it as Exception }
                BulkResponse(emptyArray<BulkItemResponse>(), 0L)
            }
            return client
        }
    }

    private val fields = parseSchemaFields(listOf("id:STRING", "name:STRING"))
    private val schema = buildSchema(fields)
    private val errorSchema = ErrorSchemas.of(schema)

    @BeforeEach
    fun setUp() = Es6Fakes.reset()

    private fun row(id: String, name: String = "张三"): Row =
        Row.withSchema(schema).addValue(id).addValue(name).build()

    private fun writeFn(batchSize: Int = 100): Elasticsearch6WriteFn {
        val fn = Elasticsearch6WriteFn(
            nodes = listOf("http://localhost:9200"),
            index = "orders",
            username = "",
            password = "",
            fields = fields,
            batchSize = batchSize,
            inputSchema = schema,
            errorSchema = errorSchema,
            deadLetter = true,
            transformName = "WriteToElasticsearch6",
        )
        fn.clientFactory = FakeClientFactory
        return fn
    }

    @Test
    fun `完整 pipeline 里写成功不产生死信，每一行都交给了 ES`() {
        val pipeline = Pipeline.create()
        val input = pipeline.apply(Create.of(row("a1"), row("a2")).withRowSchema(schema))

        val errors = input.apply(ParDo.of(writeFn())).setRowSchema(errorSchema)

        PAssert.that(errors).empty()
        pipeline.run().waitUntilFinish()

        assertEquals(2, Es6Fakes.batches.sum(), "两行都应该被提交给 ES")
    }

    @Test
    fun `完整 pipeline 里整批失败时全部行进死信，且带着原始行`() {
        Es6Fakes.bulkError = java.io.IOException("cluster blocked")
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
