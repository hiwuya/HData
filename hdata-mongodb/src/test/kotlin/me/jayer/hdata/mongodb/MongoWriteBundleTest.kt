package me.jayer.hdata.mongodb

import com.mongodb.MongoBulkWriteException
import com.mongodb.ServerAddress
import com.mongodb.bulk.BulkWriteError
import com.mongodb.bulk.BulkWriteResult
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.BulkWriteOptions
import com.mongodb.client.model.WriteModel
import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.testing.CollectingFinishBundleContext
import me.jayer.hdata.mongodb.transform.MongoWriteFn
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.Row
import org.bson.BsonDocument
import org.bson.Document
import org.joda.time.Duration
import org.joda.time.Instant
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * `MongoWriteFn` 的**攒批与死信**行为测试。只测 `toModel`（见 [MongoWriteFnTest]）是不够的，
 * 一个 bundle 里真正发生的是"攒够 batch_size 才发一次 bulkWrite""bulk 部分失败只拒绝对应下标的行"
 * "失败行进死信时带的是原始行自己的时间戳与窗口"，这些全在 `@ProcessElement` / `@FinishBundle` 里。
 *
 * 攒批这条尤其要钉住：重构前 `batch_size` 只是把行攒在内存里，真正发出去还是逐条 `insertOne`，
 * 配置看着生效了其实没有。
 *
 * 用 mock 把 driver 链打桩，不连真实库。
 *
 * @author wuya
 */
class MongoWriteBundleTest {

    private val codec = MongoRowCodec.of(listOf("id:STRING", "amount:DOUBLE"))
    private val errorSchema = ErrorSchemas.of(codec.schema)

    /** 每次 `bulkWrite` 提交进来的批次与选项，按顺序记下来。 */
    private val submitted = mutableListOf<List<WriteModel<Document>>>()
    private val options = mutableListOf<BulkWriteOptions?>()

    /** 设成非 null 后，下一次 `bulkWrite` 直接抛它。 */
    private var bulkError: Throwable? = null

    private val collection = mock<MongoCollection<Document>>()

    private fun window(): BoundedWindow = IntervalWindow(Instant(0), Duration.millis(10))

    private fun row(id: String, amount: Double = 1.0): Row =
        Row.withSchema(codec.schema).addValue(id).addValue(amount).build()

    private fun feed(fn: MongoWriteFn, vararg rows: Row) =
        rows.forEach { fn.processElement(it, Instant(1), window(), PaneInfo.NO_FIRING) }

    /** 跑完一个 bundle，返回收集到的死信输出。 */
    private fun finishBundleOf(fn: MongoWriteFn): CollectingFinishBundleContext<Row, Row> {
        val context = CollectingFinishBundleContext<Row, Row>()
        fn.finishBundle(context.context())
        return context
    }

    private fun elementOf(failureRow: Row): Row = failureRow.getValue(ErrorSchemas.ELEMENT)

    private fun mockClient(): MongoClient {
        whenever(
            collection.bulkWrite(
                anyOrNull<List<WriteModel<Document>>>(),
                anyOrNull<BulkWriteOptions>(),
            )
        ).thenAnswer { inv ->
            @Suppress("UNCHECKED_CAST")
            (inv.getArgument(0) as? List<*>)?.let { submitted.add(it as List<WriteModel<Document>>) }
            options.add(inv.getArgument(1) as BulkWriteOptions?)
            bulkError?.let { throw it } ?: BulkWriteResult.unacknowledged()
        }
        val db = mock<MongoDatabase>()
        whenever(db.getCollection(anyOrNull<String>(), anyOrNull<Class<Document>>())).doReturn(collection)
        val client = mock<MongoClient>()
        whenever(client.getDatabase(anyOrNull<String>())).doReturn(db)
        return client
    }

    private fun writeFn(batchSize: Int = 2, deadLetter: Boolean = true): MongoWriteFn {
        val fn = MongoWriteFn(
            MongoWriteConfig(
                "mongodb://localhost:27017", "db", "c",
                listOf("id:STRING", "amount:DOUBLE"),
                batchSize = batchSize,
            ),
            codec,
            errorSchema,
            deadLetter,
            "WriteToMongoDb",
        )
        fn.testClient = mockClient()
        fn.setup()
        return fn
    }

    private fun failBulkWith(vararg errors: BulkWriteError) {
        bulkError = MongoBulkWriteException(
            BulkWriteResult.unacknowledged(),
            errors.toList(),
            null,
            ServerAddress(),
            emptySet(),
        )
    }

    @Test
    fun `攒够 batch_size 才提交一次，而且一次提交整批`() {
        val fn = writeFn(batchSize = 2)

        feed(fn, row("a1"))
        assertEquals(0, submitted.size, "还没攒够一批，不该提前提交")

        feed(fn, row("a2"))

        assertEquals(1, submitted.size, "攒够 batch_size 应该只发一次 bulkWrite")
        assertEquals(2, submitted[0].size, "一次要把整批都交出去，而不是逐条")
    }

    @Test
    fun `finishBundle 把最后不足一批的行提交掉`() {
        val fn = writeFn(batchSize = 100)

        feed(fn, row("a1"))
        fn.finishBundle(CollectingFinishBundleContext<Row, Row>().context())

        assertEquals(1, submitted.size)
        assertEquals(1, submitted[0].size)
    }

    @Test
    fun `bulk 部分失败只把失败下标对应的行进死信`() {
        val fn = writeFn(batchSize = 3)
        failBulkWith(BulkWriteError(11000, "duplicate key", BsonDocument(), 1))

        feed(fn, row("a1"), row("a2"), row("a3"))
        val context = finishBundleOf(fn)

        assertEquals(1, context.outputs.size, "只有下标 1 那行失败，其余两行应算写入成功")
        assertEquals("a2", elementOf(context.outputs[0]).getString("id"))
        val message = context.outputs[0].getString(ErrorSchemas.ERROR_MESSAGE)
        assertTrue(message!!.contains("11000"), "死信里要带上真实的 Mongo 错误码，实际: $message")
    }

    @Test
    fun `死信带的是原始行自己的时间戳与窗口`() {
        val fn = writeFn(batchSize = 1)
        bulkError = IllegalStateException("connection broken")
        val stamp = Instant(4242)
        val win = IntervalWindow(Instant(100), Duration.millis(100))

        fn.processElement(row("a1"), stamp, win, PaneInfo.NO_FIRING)
        val context = finishBundleOf(fn)

        assertEquals(1, context.outputs.size)
        assertEquals(stamp, context.timestamps[0], "死信必须沿用原始行的时间戳，现编 Instant.now() 没法重放")
        assertEquals(win, context.windows[0], "死信必须沿用原始行的窗口，写死 GlobalWindow 在窗口化 pipeline 里会抛异常")
    }

    @Test
    fun `整批连接异常时全部行进死信`() {
        val fn = writeFn(batchSize = 3)
        bulkError = IllegalStateException("no reachable server")

        feed(fn, row("a1"), row("a2"))
        val context = finishBundleOf(fn)

        assertEquals(listOf("a1", "a2"), context.outputs.map { elementOf(it).getString("id") })
    }

    @Test
    fun `没开死信时写入失败直接抛异常`() {
        // 不配 schema_fields 时按 document 列写，这里故意给一个不含 document 列的 schema
        val documentCodec = MongoRowCodec.of(emptyList())
        val fn = MongoWriteFn(
            MongoWriteConfig("mongodb://localhost:27017", "db", "c", emptyList(), batchSize = 1),
            documentCodec,
            ErrorSchemas.of(documentCodec.schema),
            false,
            "WriteToMongoDb",
        )
        fn.testClient = mockClient()
        fn.setup()
        val bad = Row.withSchema(Schema.builder().addStringField("x").build()).addValue("y").build()

        assertFailsWith<IllegalArgumentException> {
            fn.processElement(bad, Instant(1), window(), PaneInfo.NO_FIRING)
        }
    }

    @Test
    fun `提交时用的是 ordered false 的 bulkWrite 选项`() {
        val fn = writeFn(batchSize = 1)

        feed(fn, row("a1"))

        // ordered=false 才能让一条失败不影响后面，也让 writeErrors 的下标能和批次对上
        assertEquals(false, options[0]?.isOrdered)
    }
}
