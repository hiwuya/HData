package me.jayer.hdata.mongodb

import com.mongodb.MongoClientSettings
import com.mongodb.bulk.BulkWriteError
import com.mongodb.bulk.WriteConcernError
import com.mongodb.client.model.InsertOneModel
import com.mongodb.client.model.ReplaceOneModel
import me.jayer.hdata.mongodb.transform.MongoWriteFn
import me.jayer.hdata.mongodb.transform.mongoBulkFailureAt
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.util.SerializableUtils
import org.apache.beam.sdk.values.Row
import org.bson.BsonDocument
import org.bson.Document
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The `upsert_keys` config must actually change the write behavior: when set, overwrite by primary key ([ReplaceOneModel]);
 * when not set, pure insert ([InsertOneModel]). This logic lives in [MongoWriteFn.toModel] and is the "declared but never
 * read" class of bug repeatedly called out in AGENTS.
 *
 * Also supplements the serialization boundaries for various DoFns / partitions / provider sources.
 *
 * @author wuya
 */
class MongoWriteFnTest {

    private val errorSchema = Schema.builder().addNullableStringField("document").build()

    private fun fn(config: MongoWriteConfig, codec: MongoRowCodec) =
        MongoWriteFn(config, codec, errorSchema, false, "WriteToMongoDb")

    /** `toModel` is a private method; trigger it via reflection to avoid making it public just for testing. */
    private fun toModel(fn: MongoWriteFn, row: Row): Any {
        val method = MongoWriteFn::class.java.getDeclaredMethod("toModel", Row::class.java)
        method.isAccessible = true
        return try {
            method.invoke(fn, row)!!
        } catch (e: java.lang.reflect.InvocationTargetException) {
            throw e.cause!!
        }
    }

    private fun row(codec: MongoRowCodec, vararg values: Any?): Row {
        val builder = Row.withSchema(codec.schema)
        values.forEach { builder.addValue(it) }
        return builder.build()
    }

    @Test
    fun `without upsert_keys it builds an InsertOneModel, a plain insert`() {
        val codec = MongoRowCodec.of(listOf("id:STRING", "amount:DOUBLE"))
        val fn = fn(MongoWriteConfig("uri", "d", "c", listOf("id:STRING", "amount:DOUBLE")), codec)

        val model = toModel(fn, row(codec, "a1", 1.0))

        assertTrue(model is InsertOneModel<*>)
        assertEquals("a1", (model as InsertOneModel<Document>).document.getString("id"))
    }

    @Test
    fun `with upsert_keys it builds a ReplaceOneModel that overwrites by primary key`() {
        val codec = MongoRowCodec.of(listOf("id:STRING", "amount:DOUBLE"))
        val config = MongoWriteConfig(
            "uri", "d", "c", listOf("id:STRING", "amount:DOUBLE"), upsertKeys = listOf("id")
        )
        val fn = fn(config, codec)

        val model = toModel(fn, row(codec, "a1", 1.0)) as ReplaceOneModel<Document>

        // The upsert filter carries the field declared in upsert_keys
        val filterJson = model.filter.toBsonDocument(
            BsonDocument::class.java, MongoClientSettings.getDefaultCodecRegistry()
        ).toJson()
        assertTrue("id" in filterJson)
        assertTrue("a1" in filterJson)
        // The document itself still carries all fields
        assertEquals(1.0, model.replacement["amount"])
    }

    @Test
    fun `upsert_keys pointing at a field absent from the document fails fast`() {
        // validate would catch upsert_keys not in schema_fields, but constructing the config while bypassing validate can still trigger
        // the defensive require in toModel — if this path is not guarded, the runtime bulkWrite would treat a bunch of rows as the same primary key and overwrite them
        val codec = MongoRowCodec.of(listOf("id:STRING"))
        val config = MongoWriteConfig(
            "uri", "d", "c", listOf("id:STRING"), upsertKeys = listOf("id", "order_no")
        )
        val fn = fn(config, codec)

        val error = assertFailsWith<IllegalArgumentException> { toModel(fn, row(codec, "a1")) }

        assertTrue("order_no" in error.message!!)
    }

    @Test
    fun `a bulk per-row error only rejects the matching index`() {
        val errors = listOf(BulkWriteError(11000, "duplicate key", BsonDocument(), 1))

        assertNull(mongoBulkFailureAt(errors, null, 0))
        val failure = mongoBulkFailureAt(errors, null, 1)
        assertTrue(failure != null && "11000" in failure.message!!)
    }

    @Test
    fun `a write concern error makes the whole batch unconfirmable`() {
        val concern = WriteConcernError(64, "WriteConcernFailed", "replication timeout", BsonDocument())
        val row0 = mongoBulkFailureAt(emptyList(), concern, 0)
        val row1 = mongoBulkFailureAt(emptyList(), concern, 1)

        assertTrue(row0 != null && "write concern" in row0.message!!)
        assertTrue(row1 != null && "cannot be confirmed" in row1.message!!)
    }

    @Test
    fun `the write DoFn can be serialized and shipped`() {
        val codec = MongoRowCodec.of(listOf("id:STRING"))
        val fn = fn(MongoWriteConfig("uri", "d", "c", listOf("id:STRING")), codec)
        SerializableUtils.ensureSerializable(fn)
    }
}
