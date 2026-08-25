package me.jayer.hdata.mongodb

import com.mongodb.client.FindIterable
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoCursor
import com.mongodb.client.MongoDatabase
import me.jayer.hdata.core.testing.CollectingOutputReceiver
import me.jayer.hdata.mongodb.transform.MongoReadFn
import me.jayer.hdata.mongodb.transform.MongoReadSplit
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.bson.BsonDocument
import org.bson.Document
import kotlin.test.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever

/**
 * 验证 LIMIT 真的下推到驱动（`find().limit()`），而不是在 Beam 侧事后截断——
 * 这样 MongoDB 才不会因为 limit 而把多余文档传过来。用 mock 把整条 driver 链打桩，
 * 不连真实库。
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
}
