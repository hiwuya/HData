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
 * End-to-end test of `HBaseWriteFn` running inside a **full pipeline** (DirectRunner).
 *
 * The fake `Table` is a `java.lang.reflect.Proxy`: `Table` is an interface, and as long as the
 * `InvocationHandler` is serializable, the proxy itself **can be serialized and shipped along with the
 * DoFn** (a Mockito mock cannot, by default).
 *
 * @author wuya
 */
class HBasePipelineTest {

    object HBaseFakes {
        val batches: MutableList<Int> = Collections.synchronizedList(mutableListOf())

        /** Row indexes that get marked as failed in `results`. */
        @Volatile
        var failAt: Set<Int> = emptySet()

        /** When non-null, the whole batch throws it after filling in results. */
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
                // results[i] being a Throwable means that row failed, null means "this row was never attempted",
                // anything else means success -- so a successful row must be filled with something non-null,
                // otherwise it is treated as never attempted
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

    private fun row(rowkey: String, name: String? = "Alice", age: Int? = 30): Row =
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
    fun `a successful full-pipeline write produces no dead letters, and every row reaches HBase`() {
        val pipeline = Pipeline.create()
        val input = pipeline.apply(Create.of(row("r1"), row("r2")).withRowSchema(codec.schema))

        val errors = input.apply(ParDo.of(writeFn())).setRowSchema(errorSchema)

        PAssert.that(errors).empty()
        pipeline.run().waitUntilFinish()

        // How DirectRunner splits bundles is its own business, so we only assert "both rows were handed over"
        assertEquals(2, HBaseFakes.batches.sum(), "both rows should have been submitted to HBase")
    }

    @Test
    fun `in a full pipeline only the row at the failing index goes to the dead letter, carrying the original row`() {
        HBaseFakes.failAt = setOf(0)
        val pipeline = Pipeline.create()
        val input = pipeline.apply(Create.of(row("r1")).withRowSchema(codec.schema))

        val errors = input.apply(ParDo.of(writeFn())).setRowSchema(errorSchema)

        PAssert.that(errors).satisfies { rows ->
            val failures = rows.toList()
            assertEquals(1, failures.size)
            assertEquals("r1", failures[0].getValue<Row>(ErrorSchemas.ELEMENT).getString("rowkey"))
            val message = failures[0].getString(ErrorSchemas.ERROR_MESSAGE)
            assertTrue(message!!.contains("region offline"), "the dead letter should carry the real exception, got: $message")
            null
        }
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `a row whose columns are all null is rejected while building the Put, not sent to HBase, and does not fail the job`() {
        val pipeline = Pipeline.create()
        val empty = Row.withSchema(codec.schema).addValue("r1").addValue(null).addValue(null).build()
        val input = pipeline.apply(Create.of(empty).withRowSchema(codec.schema))

        val errors = input.apply(ParDo.of(writeFn())).setRowSchema(errorSchema)

        PAssert.that(errors).satisfies { rows -> assertEquals(1, rows.toList().size); null }
        pipeline.run().waitUntilFinish()

        assertEquals(0, HBaseFakes.batches.sum(), "HBase rejects an empty Put, it should never be sent")
    }
}
