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
    fun `aggregate 模式走 aggregate 管道而非 find，且结果映射成 Row`() {
        val cursor = mock<MongoCursor<Document>>()
        whenever(cursor.hasNext()).doReturn(true)
        whenever(cursor.next()).doReturn(Document().append("total", 7L).append("min_amount", 1.5))
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
        val fn = MongoReadFn("mongodb://x", MongoRowCodec.of(emptyList()), 100, -1, specs)
        fn.testClient = client
        fn.setup()
        val receiver = CollectingOutputReceiver<Row>()
        fn.processElement(readSplit(), OffsetRangeTracker(OffsetRange(0, 1)), receiver)

        verify(collection).aggregate(any<List<Document>>(), anyOrNull<Class<Document>>())
        verify(collection, never()).find(anyOrNull<BsonDocument>())
        assertEquals(1, receiver.outputs.size)
        val row = receiver.outputs[0] as Row
        assertEquals(7L, row.getInt64("total"))
        assertEquals(1.5, row.getDouble("min_amount"))
    }
}
