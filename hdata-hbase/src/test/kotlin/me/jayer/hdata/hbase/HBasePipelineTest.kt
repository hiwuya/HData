package me.jayer.hdata.hbase

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.hbase.transform.HBaseWriteFn
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.hbase.client.Table
import org.junit.jupiter.api.BeforeEach
import java.io.IOException
import java.io.Serializable
import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.util.Collections
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * `HBaseWriteFn` 跑在**完整 pipeline**（DirectRunner）上的端到端测试。
 *
 * 假 `Table` 用 `java.lang.reflect.Proxy`：`Table` 是接口，而 `Proxy` 只要 `InvocationHandler`
 * 可序列化，代理本身就能**跟着 DoFn 一起序列化下发**（Mockito 的 mock 默认不行）。
 *
 * @author wuya
 */
class HBasePipelineTest {

    object HBaseFakes {
        val batches: MutableList<Int> = Collections.synchronizedList(mutableListOf())

        /** 这些下标的行在 `results` 里被记成失败。 */
        @Volatile
        var failAt: Set<Int> = emptySet()

        /** 非 null 时，整批在填完 results 之后抛它。 */
        @Volatile
        var batchError: Throwable? = null

        fun reset() {
            batches.clear()
            failAt = emptySet()
            batchError = null
        }
    }

    private object TableHandler : InvocationHandler, Serializable {
        override fun invoke(proxy: Any, method: Method, args: Array<Any?>?): Any? = when (method.name) {
            "batch" -> {
                val actions = args!![0] as List<*>
                HBaseFakes.batches.add(actions.size)
                val results = args[1] as Array<Any?>
                // results[i] 是 Throwable 记失败、null 记"这行根本没被尝试"、其余记成功，
                // 所以成功的行必须填个非 null 的东西，否则会被当成没尝试
                results.indices.forEach { i ->
                    results[i] = if (i in HBaseFakes.failAt) IOException("region offline") else Any()
                }
                HBaseFakes.batchError?.let { throw it }
                null
            }
            "hashCode" -> System.identityHashCode(proxy)
            "equals" -> proxy === args?.get(0)
            "toString" -> "fake-table"
            else -> null
        }
    }

    private val codec = HBaseRowCodec.of("rowkey", "string", listOf("name:STRING", "age:INT32"), "cf")
    private val errorSchema = ErrorSchemas.of(codec.schema)

    @BeforeEach
    fun setUp() = HBaseFakes.reset()

    private fun row(rowkey: String, name: String? = "张三", age: Int? = 30): Row =
        Row.withSchema(codec.schema).addValue(rowkey).addValue(name).addValue(age).build()

    private fun writeFn(batchSize: Int = 100): HBaseWriteFn {
        val fn = HBaseWriteFn(
            HBaseWriteConfig(
                zookeeperQuorum = "localhost:2181",
                table = "orders",
                rowkeyField = "rowkey",
                family = "cf",
                schemaFields = listOf("name:STRING", "age:INT32"),
                batchSize = batchSize,
            ),
            codec,
            errorSchema,
            true,
            "WriteToHBase",
        )
        fn.testTable = Proxy.newProxyInstance(
            Table::class.java.classLoader, arrayOf(Table::class.java), TableHandler
        ) as Table
        return fn
    }

    @Test
    fun `完整 pipeline 里写成功不产生死信，每一行都交给了 HBase`() {
        val pipeline = Pipeline.create()
        val input = pipeline.apply(Create.of(row("r1"), row("r2")).withRowSchema(codec.schema))

        val errors = input.apply(ParDo.of(writeFn())).setRowSchema(errorSchema)

        PAssert.that(errors).empty()
        pipeline.run().waitUntilFinish()

        // DirectRunner 怎么切 bundle 是它自己的事，所以只断言"两行都交出去了"
        assertEquals(2, HBaseFakes.batches.sum(), "两行都应该被提交给 HBase")
    }

    @Test
    fun `完整 pipeline 里只有失败下标那行进死信，且带着原始行`() {
        HBaseFakes.failAt = setOf(0)
        val pipeline = Pipeline.create()
        val input = pipeline.apply(Create.of(row("r1")).withRowSchema(codec.schema))

        val errors = input.apply(ParDo.of(writeFn())).setRowSchema(errorSchema)

        PAssert.that(errors).satisfies { rows ->
            val failures = rows.toList()
            assertEquals(1, failures.size)
            assertEquals("r1", failures[0].getValue<Row>(ErrorSchemas.ELEMENT).getString("rowkey"))
            val message = failures[0].getString(ErrorSchemas.ERROR_MESSAGE)
            assertTrue(message!!.contains("region offline"), "死信要带真实异常，实际: $message")
            null
        }
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `列全为 null 的行在构 Put 时就被拒，不发给 HBase 也不弄挂作业`() {
        val pipeline = Pipeline.create()
        val empty = Row.withSchema(codec.schema).addValue("r1").addValue(null).addValue(null).build()
        val input = pipeline.apply(Create.of(empty).withRowSchema(codec.schema))

        val errors = input.apply(ParDo.of(writeFn())).setRowSchema(errorSchema)

        PAssert.that(errors).satisfies { rows -> assertEquals(1, rows.toList().size); null }
        pipeline.run().waitUntilFinish()

        assertEquals(0, HBaseFakes.batches.sum(), "HBase 拒收空 Put，不该发出去")
    }
}
