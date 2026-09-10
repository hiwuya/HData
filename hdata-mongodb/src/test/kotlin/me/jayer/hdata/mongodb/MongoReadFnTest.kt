package me.jayer.hdata.mongodb

import com.mongodb.client.AggregateIterable
import com.mongodb.client.FindIterable
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoCursor
import com.mongodb.client.MongoDatabase
import me.jayer.hdata.core.testing.CollectingOutputReceiver
import me.jayer.hdata.mongodb.MongoAggregateSpec
import me.jayer.hdata.mongodb.MongoRowCodec
import me.jayer.hdata.mongodb.transform.MongoPartialAggregateFn
import me.jayer.hdata.mongodb.transform.MongoReadFn
import me.jayer.hdata.mongodb.transform.MongoReadSplit
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.values.Row
import org.bson.BsonDocument
import org.bson.Document
import kotlin.test.Test
import kotlin.test.assertEquals
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever

/**
 * 验证下推真的落到驱动层，而不是在 Beam 侧事后截断：
 *  - LIMIT 下推成 `find().limit()`；
 *  - 聚合下推走 `aggregate(...)` 管道而非 `find`。
 * 用 mock 把整条 driver 链打桩，不连真实库。
 */
class MongoReadFnTest {

    private data class MockChain(val client: MongoClient, val iterable: FindIterable<Document>)

    private fun codec() = MongoRowCodec.of(emptyList())

    private fun readSplit() = MongoReadSplit("db", "c", listOf("{}"))

    private fun mockChain(): MockChain {
        val cursor = mock<MongoCursor<Document>>()
        whenever(cursor.hasNext()).doReturn(false)
        val iterable = mock<FindIterable<Document>>()
        whenever(iterable.projection(anyOrNull())).doReturn(iterable)
        whenever(iterable.batchSize(anyOrNull())).doReturn(iterable)
        whenever(iterable.limit(anyOrNull())).doReturn(iterable)
        whenever(iterable.iterator()).doReturn(cursor)
        val collection = mock<MongoCollection<Document>>()
        whenever(collection.find(anyOrNull<BsonDocument>())).doReturn(iterable)
        val db = mock<MongoDatabase>()
        whenever(db.getCollection(anyOrNull<String>(), anyOrNull<Class<Document>>())).doReturn(collection)
        val client = mock<MongoClient>()
        whenever(client.getDatabase(anyOrNull<String>())).doReturn(db)
        return MockChain(client, iterable)
    }

    /** 给出一个会依次吐出 [docs] 的游标所在的 find 链。 */
    private fun iterableOf(vararg docs: Document): FindIterable<Document> {
        var cursor = 0
        val cursorMock = mock<MongoCursor<Document>>()
        whenever(cursorMock.hasNext()).thenAnswer { cursor < docs.size }
        whenever(cursorMock.next()).thenAnswer { docs[cursor++] }
        val iterable = mock<FindIterable<Document>>()
        whenever(iterable.projection(anyOrNull())).doReturn(iterable)
        whenever(iterable.batchSize(anyOrNull())).doReturn(iterable)
        whenever(iterable.limit(anyOrNull())).doReturn(iterable)
        whenever(iterable.iterator()).doReturn(cursorMock)
        return iterable
    }

    /** 每个分片返回不同结果的 find 链，用来验证"逐个分片认领"真的走到了每一片。 */
    private fun multiPartitionChain(iterables: List<FindIterable<Document>>): MongoCollection<Document> {
        var call = 0
        val collection = mock<MongoCollection<Document>>()
        whenever(collection.find(anyOrNull<BsonDocument>())).thenAnswer { iterables[call++] }
        val db = mock<MongoDatabase>()
        whenever(db.getCollection(anyOrNull<String>(), anyOrNull<Class<Document>>())).doReturn(collection)
        val client = mock<MongoClient>()
        whenever(client.getDatabase(anyOrNull<String>())).doReturn(db)
        return collection
    }

    @Test
    fun `splitRestriction 把分片区间按每个分片一份切开`() {
        val fn = MongoReadFn("mongodb://x", codec(), 100)
        val split = MongoReadSplit("db", "c", listOf("{}", "{}", "{}"))
        val receiver = CollectingOutputReceiver<OffsetRange>()

        fn.splitRestriction(split, OffsetRange(0, 3), receiver)

        // 分片已经按 $bucketAuto 均衡过，一片一份即可；切成更多份只会增加调度开销
        assertEquals(3, receiver.outputs.size)
        assertEquals(listOf(0L, 1L, 2L), receiver.outputs.map { it.from })
        assertEquals(listOf(1L, 2L, 3L), receiver.outputs.map { it.to })
    }

    @Test
    fun `空分片区间不产出任何子区间`() {
        val fn = MongoReadFn("mongodb://x", codec(), 100)
        val receiver = CollectingOutputReceiver<OffsetRange>()

        fn.splitRestriction(readSplit(), OffsetRange(0, 0), receiver)

        assertEquals(0, receiver.outputs.size)
    }

    @Test
    fun `processElement 逐个分片认领，每个分片的文档都读出来`() {
        val codec = MongoRowCodec.of(listOf("id:STRING"))
        val collection = multiPartitionChain(
            listOf(
                iterableOf(Document("id", "a1"), Document("id", "a2")),
                iterableOf(Document("id", "b1")),
            )
        )
        val fn = MongoReadFn("mongodb://x", codec, 100)
        fn.testClient = mockChainOf(collection)
        fn.setup()
        val receiver = CollectingOutputReceiver<Row>()

        fn.processElement(
            MongoReadSplit("db", "c", listOf("{}", "{}")),
            OffsetRangeTracker(OffsetRange(0, 2)),
            receiver,
        )

        assertEquals(listOf("a1", "a2", "b1"), receiver.outputs.map { it.getString("id") })
        verify(collection, times(2)).find(anyOrNull<BsonDocument>())
    }

    @Test
    fun `认领被拒时立刻停手，不再往下读`() {
        val collection = multiPartitionChain(
            listOf(iterableOf(Document("id", "a1")), iterableOf(Document("id", "b1")))
        )
        val fn = MongoReadFn("mongodb://x", MongoRowCodec.of(listOf("id:STRING")), 100)
        fn.testClient = mockChainOf(collection)
        fn.setup()
        val receiver = CollectingOutputReceiver<Row>()
        // 运行时把剩下的活切走时就会拒掉本次认领，此时必须停手而不是继续读
        val refusing = object : OffsetRangeTracker(OffsetRange(0, 2)) {
            override fun tryClaim(position: Long): Boolean = false
        }

        fn.processElement(MongoReadSplit("db", "c", listOf("{}", "{}")), refusing, receiver)

        assertEquals(0, receiver.outputs.size)
        verify(collection, never()).find(anyOrNull<BsonDocument>())
    }

    private fun mockChainOf(collection: MongoCollection<Document>): MongoClient {
        val db = mock<MongoDatabase>()
        whenever(db.getCollection(anyOrNull<String>(), anyOrNull<Class<Document>>())).doReturn(collection)
        val client = mock<MongoClient>()
        whenever(client.getDatabase(anyOrNull<String>())).doReturn(db)
        return client
    }

    @Test
    fun `limit 大于 0 时下推 find limit`() {
        val (client, iterable) = mockChain()
        val fn = MongoReadFn("mongodb://x", codec(), 100, limit = 5)
        fn.testClient = client
        fn.setup()
        fn.processElement(readSplit(), OffsetRangeTracker(OffsetRange(0, 1)), CollectingOutputReceiver())

        verify(iterable).limit(5)
    }

    @Test
    fun `limit 不限制时不调用 find limit`() {
        val (client, iterable) = mockChain()
        val fn = MongoReadFn("mongodb://x", codec(), 100, limit = -1)
        fn.testClient = client
        fn.setup()
        fn.processElement(readSplit(), OffsetRangeTracker(OffsetRange(0, 1)), CollectingOutputReceiver())

        verify(iterable, never()).limit(any())
    }

    @Test
    fun `超大 limit 不会窄化成负数传给 Mongo driver`() {
        val huge = Int.MAX_VALUE.toLong() + 1
        assertEquals(null, MongoReadFn.cursorLimit(huge))
        assertEquals(1000, MongoReadFn.cursorLimit(1000))

        val (client, iterable) = mockChain()
        val fn = MongoReadFn("mongodb://x", codec(), 100, limit = huge)
        fn.testClient = client
        fn.setup()
        fn.processElement(readSplit(), OffsetRangeTracker(OffsetRange(0, 1)), CollectingOutputReceiver())

        verify(iterable, never()).limit(any())
    }

    @Test
    fun `聚合下推走 aggregate 管道而非 find，且每个分片产出局部 PartialAgg`() {
        val cursor = mock<MongoCursor<Document>>()
        whenever(cursor.hasNext()).doReturn(true)
        whenever(cursor.next()).doReturn(
            Document("total", 7L).append("min_amount", 1.5),
        )
        val iter = mock<AggregateIterable<Document>>()
        whenever(iter.allowDiskUse(anyOrNull<Boolean>())).doReturn(iter)
        whenever(iter.iterator()).doReturn(cursor)
        val collection = mock<MongoCollection<Document>>()
        whenever(collection.aggregate(any<List<Document>>(), anyOrNull<Class<Document>>())).doReturn(iter)
        val db = mock<MongoDatabase>()
        whenever(db.getCollection(anyOrNull<String>(), anyOrNull<Class<Document>>())).doReturn(collection)
        val client = mock<MongoClient>()
        whenever(client.getDatabase(anyOrNull<String>())).doReturn(db)

        val specs = listOf(
            MongoAggregateSpec("count", "", "total"),
            MongoAggregateSpec("min", "amount", "min_amount"),
        )
        val fn = MongoPartialAggregateFn("mongodb://x", specs)
        fn.testClient = client
        fn.setup()
        val receiver = CollectingOutputReceiver<PartialAgg>()
        fn.processElement(readSplit(), OffsetRangeTracker(OffsetRange(0, 1)), receiver)

        verify(collection).aggregate(any<List<Document>>(), anyOrNull<Class<Document>>())
        verify(collection, never()).find(anyOrNull<BsonDocument>())
        assertEquals(1, receiver.outputs.size)
        val partial = receiver.outputs[0]
        assertEquals(7L, partial.count)
        assertEquals(1.5, partial.mins["min_amount"])
        verify(cursor).close()
    }
}
