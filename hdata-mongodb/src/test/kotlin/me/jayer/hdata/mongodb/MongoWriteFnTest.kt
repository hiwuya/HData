package me.jayer.hdata.mongodb

import com.mongodb.MongoClientSettings
import com.mongodb.client.model.InsertOneModel
import com.mongodb.client.model.ReplaceOneModel
import me.jayer.hdata.mongodb.transform.MongoWriteFn
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.util.SerializableUtils
import org.apache.beam.sdk.values.Row
import org.bson.BsonDocument
import org.bson.Document
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * `upsert_keys` 配置必须真的改变写行为：配了就按主键覆盖写（[ReplaceOneModel]），没配就纯插入
 * （[InsertOneModel]）。这段逻辑在 [MongoWriteFn.toModel] 里，是 AGENTS 反复点名的"声明了却没读"类 bug。
 *
 * 另外补上各类 DoFn / 分片 / provider source 的序列化边界。
 *
 * @author wuya
 */
class MongoWriteFnTest {

    private val errorSchema = Schema.builder().addNullableStringField("document").build()

    private fun fn(config: MongoWriteConfig, codec: MongoRowCodec) =
        MongoWriteFn(config, codec, errorSchema, false, "WriteToMongoDb")

    /** `toModel` 是私有方法，用反射触发它，避免为了测试把它改成 public。 */
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
    fun `未配 upsert_keys 时生成 InsertOneModel，纯插入`() {
        val codec = MongoRowCodec.of(listOf("id:STRING", "amount:DOUBLE"))
        val fn = fn(MongoWriteConfig("uri", "d", "c", listOf("id:STRING", "amount:DOUBLE")), codec)

        val model = toModel(fn, row(codec, "a1", 1.0))

        assertTrue(model is InsertOneModel<*>)
        assertEquals("a1", (model as InsertOneModel<Document>).document.getString("id"))
    }

    @Test
    fun `配了 upsert_keys 时生成按主键覆盖写的 ReplaceOneModel`() {
        val codec = MongoRowCodec.of(listOf("id:STRING", "amount:DOUBLE"))
        val config = MongoWriteConfig(
            "uri", "d", "c", listOf("id:STRING", "amount:DOUBLE"), upsertKeys = listOf("id")
        )
        val fn = fn(config, codec)

        val model = toModel(fn, row(codec, "a1", 1.0)) as ReplaceOneModel<Document>

        // upsert 的过滤条件里带上了 upsert_keys 声明的字段
        val filterJson = model.filter.toBsonDocument(
            BsonDocument::class.java, MongoClientSettings.getDefaultCodecRegistry()
        ).toJson()
        assertTrue("id" in filterJson)
        assertTrue("a1" in filterJson)
        // 文档本身也照常带全部字段
        assertEquals(1.0, model.replacement["amount"])
    }

    @Test
    fun `upsert_keys 指向文档里不存在的字段时直接报错`() {
        // upsert_keys 不在 schema_fields 里时 validate 会拦，但绕过 validate 直接构造配置仍能触发
        // toModel 里的防御性 require——这条路径不守的话，运行时 bulkWrite 会把一堆行当成同一个主键覆盖掉
        val codec = MongoRowCodec.of(listOf("id:STRING"))
        val config = MongoWriteConfig(
            "uri", "d", "c", listOf("id:STRING"), upsertKeys = listOf("id", "order_no")
        )
        val fn = fn(config, codec)

        val error = assertFailsWith<IllegalArgumentException> { toModel(fn, row(codec, "a1")) }

        assertTrue("order_no" in error.message!!)
    }

    @Test
    fun `写入 DoFn 可序列化下发`() {
        val codec = MongoRowCodec.of(listOf("id:STRING"))
        val fn = fn(MongoWriteConfig("uri", "d", "c", listOf("id:STRING")), codec)
        SerializableUtils.ensureSerializable(fn)
    }
}
