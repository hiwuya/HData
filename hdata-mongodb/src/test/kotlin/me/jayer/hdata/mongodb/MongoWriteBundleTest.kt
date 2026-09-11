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
 * Behavior tests for `MongoWriteFn`'s **batching and dead letter**. Testing only `toModel` (see [MongoWriteFnTest])
 * is not enough — what actually happens in a bundle is "only fire one bulkWrite once batch_size is reached",
 * "bulk partial failure only rejects the row at the corresponding index", and "dead-lettered rows carry the original
 * row's own timestamp and window", all of which live in `@ProcessElement` / `@FinishBundle`.
 *
 * The batching one must be pinned down especially: before the refactor `batch_size` only buffered rows in memory,
 * but what was actually sent was still per-row `insertOne` — the config looked effective but wasn't.
 *
 * Uses mocks to stub the driver chain, without connecting to a real database.
 *
 * @author wuya
 */
class MongoWriteBundleTest {

    private val codec = MongoRowCodec.of(listOf("id:STRING", "amount:DOUBLE"))
    private val errorSchema = ErrorSchemas.of(codec.schema)

    /** The batches and options submitted by each `bulkWrite`, recorded in order. */
    private val submitted = mutableListOf<List<WriteModel<Document>>>()
    private val options = mutableListOf<BulkWriteOptions?>()

    /** Once set to non-null, the next `bulkWrite` throws it directly. */
    private var bulkError: Throwable? = null

    private val collection = mock<MongoCollection<Document>>()

    private fun window(): BoundedWindow = IntervalWindow(Instant(0), Duration.millis(10))

    private fun row(id: String, amount: Double = 1.0): Row =
        Row.withSchema(codec.schema).addValue(id).addValue(amount).build()

    private fun feed(fn: MongoWriteFn, vararg rows: Row) =
        rows.forEach { fn.processElement(it, Instant(1), window(), PaneInfo.NO_FIRING) }

    /** Runs a bundle to completion and returns the collected dead-letter output. */
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
    fun `it commits only once batch_size is reached, and commits the whole batch at once`() {
        val fn = writeFn(batchSize = 2)

        feed(fn, row("a1"))
        assertEquals(0, submitted.size, "Batch not full yet, should not submit early")

        feed(fn, row("a2"))

        assertEquals(1, submitted.size, "Once batch_size is reached, only one bulkWrite should be sent")
        assertEquals(2, submitted[0].size, "The whole batch must be handed off at once, not row by row")
    }

    @Test
    fun `finishBundle commits the trailing rows that do not fill a batch`() {
        val fn = writeFn(batchSize = 100)

        feed(fn, row("a1"))
        fn.finishBundle(CollectingFinishBundleContext<Row, Row>().context())

        assertEquals(1, submitted.size)
        assertEquals(1, submitted[0].size)
    }

    @Test
    fun `a partial bulk failure dead-letters only the row at the failing index`() {
        val fn = writeFn(batchSize = 3)
        failBulkWith(BulkWriteError(11000, "duplicate key", BsonDocument(), 1))

        feed(fn, row("a1"), row("a2"), row("a3"))
        val context = finishBundleOf(fn)

        assertEquals(1, context.outputs.size, "Only the row at index 1 failed; the other two should count as written successfully")
        assertEquals("a2", elementOf(context.outputs[0]).getString("id"))
        val message = context.outputs[0].getString(ErrorSchemas.ERROR_MESSAGE)
        assertTrue(message!!.contains("11000"), "The dead letter must carry the real Mongo error code, actual: $message")
    }

    @Test
    fun `the dead letter carries the original row's own timestamp and window`() {
        val fn = writeFn(batchSize = 1)
        bulkError = IllegalStateException("connection broken")
        val stamp = Instant(4242)
        val win = IntervalWindow(Instant(100), Duration.millis(100))

        fn.processElement(row("a1"), stamp, win, PaneInfo.NO_FIRING)
        val context = finishBundleOf(fn)

        assertEquals(1, context.outputs.size)
        assertEquals(stamp, context.timestamps[0], "Dead letter must reuse the original row's timestamp; inventing Instant.now() cannot be replayed")
        assertEquals(win, context.windows[0], "Dead letter must reuse the original row's window; hard-coding GlobalWindow throws in a windowed pipeline")
    }

    @Test
    fun `when the whole batch hits a connection error every row goes to dead letter`() {
        val fn = writeFn(batchSize = 3)
        bulkError = IllegalStateException("no reachable server")

        feed(fn, row("a1"), row("a2"))
        val context = finishBundleOf(fn)

        assertEquals(listOf("a1", "a2"), context.outputs.map { elementOf(it).getString("id") })
    }

    @Test
    fun `with dead letter disabled a write failure throws directly`() {
        // When schema_fields is not configured, write by the document column; here we deliberately give a schema without a document column
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
    fun `the commit uses bulkWrite options with ordered false`() {
        val fn = writeFn(batchSize = 1)

        feed(fn, row("a1"))

        // ordered=false lets one failure not affect the rest, and lets the writeErrors index line up with the batch
        assertEquals(false, options[0]?.isOrdered)
    }
}
