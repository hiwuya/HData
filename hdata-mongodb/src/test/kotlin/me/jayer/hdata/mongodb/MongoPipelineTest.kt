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
 * `MongoWriteFn` 跑在**完整 pipeline**（DirectRunner）上的端到端测试。
 *
 * 只调 `processElement` 的单测发现不了两类问题：DoFn 里夹了不可序列化的东西（提交作业时才炸），
 * 以及攒批/死信在 Beam 的 bundle 生命周期下行为不对。这里两者都覆盖到。
 *
 * 假客户端用 `java.lang.reflect.Proxy` 而不是 Mockito：MongoDB 驱动对外全是接口，
 * 而 `Proxy` 只要 `InvocationHandler` 可序列化，代理本身就**跟着 DoFn 一起序列化下发**；
 * Mockito 的 mock 默认不可序列化，注入的假客户端会在 worker 上凭空消失。
 *
 * @author wuya
 */
class MongoPipelineTest {

    /** 假客户端的记录都落在这里：反序列化回来的 handler 是另一个实例，只有静态状态能跨 JVM 边界对齐。 */
    object MongoFakes {
        val batches: MutableList<List<Document>> = Collections.synchronizedList(mutableListOf())

        /** 非 null 时，下一次 `bulkWrite` 抛它。 */
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
    fun `完整 pipeline 里写成功不产生死信，每一行都交给了 MongoDB`() {
        val pipeline = org.apache.beam.sdk.Pipeline.create()
        val input = pipeline.apply(Create.of(row("a1"), row("a2")).withRowSchema(codec.schema))

        val errors = input.apply(ParDo.of(writeFn(batchSize = 100))).setRowSchema(errorSchema)

        PAssert.that(errors).empty()
        pipeline.run().waitUntilFinish()

        // DirectRunner 怎么切 bundle 是它自己的事，所以这里只看"两行都交出去了"，
        // "攒够一批才提交一次"由 MongoWriteBundleTest 直接调 processElement 钉住
        assertEquals(
            listOf("a1", "a2"),
            MongoFakes.batches.flatten().map { it.getString("id") }.sortedBy { it },
        )
    }

    @Test
    fun `完整 pipeline 里写失败的行进死信，且带着原始行`() {
        MongoFakes.bulkError = IllegalStateException("no reachable server")
        val pipeline = org.apache.beam.sdk.Pipeline.create()
        val input = pipeline.apply(Create.of(row("a1"), row("a2")).withRowSchema(codec.schema))

        val errors = input.apply(ParDo.of(writeFn(batchSize = 100))).setRowSchema(errorSchema)

        PAssert.that(errors).satisfies { rows ->
            val failures = rows.toList()
            assertEquals(2, failures.size, "整批失败时两行都该进死信")
            val ids = failures.map { it.getValue<Row>(ErrorSchemas.ELEMENT).getString("id") }.sortedBy { it }
            assertEquals(listOf("a1", "a2"), ids, "死信里必须是原始行，丢了就没法重放")
            val message = failures[0].getString(ErrorSchemas.ERROR_MESSAGE)
            assertTrue(message!!.contains("no reachable server"), "死信要带真实异常，实际: $message")
            null
        }
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `行里缺 schema_fields 声明的列时只拒这一行，不把作业弄挂`() {
        val partial = Schema.builder().addNullableStringField("id").build()
        val bad = Row.withSchema(partial).addValue("a1").build()
        val pipeline = org.apache.beam.sdk.Pipeline.create()
        val input = pipeline.apply(Create.of(bad).withRowSchema(partial))

        // 输入行缺 codec 声明的 amount 列，toModel 会直接拒这一行走死信，而不是让整作业失败
        val errors = input.apply(ParDo.of(writeFn(batchSize = 2)))
            .setRowSchema(ErrorSchemas.of(partial))

        PAssert.that(errors).satisfies { rows -> assertEquals(1, rows.toList().size); null }
        pipeline.run().waitUntilFinish()
        assertTrue(MongoFakes.batches.isEmpty(), "被拒的行不该发给 MongoDB")
    }


    private companion object {
        @Suppress("UNCHECKED_CAST")
        fun <T> proxyOf(iface: Class<T>, handler: InvocationHandler): T =
            Proxy.newProxyInstance(iface.classLoader, arrayOf(iface), handler) as T
    }
}
