package me.jayer.hdata.mongodb.internal

import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoCollection
import org.bson.BsonDocument
import org.bson.Document
import org.bson.conversions.Bson
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import org.slf4j.LoggerFactory
import kotlin.math.ceil

/**
 * 把一个集合切成若干互不重叠的 `_id` 区间。
 *
 * 重构前的切分是 `sort(_id).skip(n).limit(m)`，有两个要命的问题：
 *  1. **每个分片都要从头重扫**：读第 k 个分片时 MongoDB 得先跳过前 k·m 个文档，
 *     总代价是 O(n²)，分片一多就比不分片还慢；
 *  2. **不是稳定切分**：`countDocuments()` 与各分片实际执行之间只要有插入或删除，
 *     分片边界就会整体移位，结果是有的文档读两遍、有的一遍都读不到，而作业照常成功退出。
 *
 * 现在改成先用 `$bucketAuto` 求出 `_id` 的分桶边界，再把边界翻译成 `_id >= a AND _id < b` 的过滤条件——
 * 每个分片都走 `_id` 索引，而且边界是具体的值，不受并发写入影响。这也是 Beam 自带的
 * `MongoDbIO.BoundedMongoDbSource` 采用的切法。
 *
 * @author wuya
 */
internal object MongoBuckets {

    private val LOGGER = LoggerFactory.getLogger(MongoBuckets::class.java)

    /** 每个分片的目标文档数，用来在没显式指定 partition_num 时估算桶数。 */
    private const val DOCS_PER_PARTITION = 100_000L

    private const val MAX_PARTITIONS = 1000

    /** 扩展模式的 JSON：ObjectId / Long / Decimal128 这些类型在往返后仍然是原类型。 */
    private val JSON_SETTINGS: JsonWriterSettings =
        JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build()

    /**
     * @return 每个分片的完整过滤条件（扩展 JSON），已经把用户的 [filter] 与 `_id` 区间 and 在一起。
     *         集合为空时返回空列表。
     */
    fun partitionFilters(
        collection: MongoCollection<Document>,
        filter: Bson?,
        partitionNum: Int?,
    ): List<String> {
        val total = if (filter == null) collection.countDocuments() else collection.countDocuments(filter)
        if (total <= 0) {
            LOGGER.info("集合[{}] 没有匹配的文档", collection.namespace)
            return emptyList()
        }
        val buckets = partitionNum ?: autoPartitionNum(total)
        if (buckets <= 1) {
            return listOf(toJson(filter ?: BsonDocument()))
        }

        val bounds = bucketBounds(collection, filter, buckets)
        if (bounds.size <= 1) {
            LOGGER.info("集合[{}] 分不出多个桶，退化为单分片读", collection.namespace)
            return listOf(toJson(filter ?: BsonDocument()))
        }

        LOGGER.info("集合[{}] 共 {} 个文档，切成 {} 个分片", collection.namespace, total, bounds.size)
        return rangesToFilters(bounds, filter)
    }

    /**
     * `$bucketAuto` 给出的 `(min, max)` 列表翻译成互不重叠的过滤条件。
     *
     * 拆成独立函数是为了能脱离 MongoDB 单测——分片边界算错的后果是漏数据或重复数据，
     * 而这两种后果都不会让作业失败，只能靠测试守住。
     */
    fun rangesToFilters(ranges: List<Pair<Any?, Any?>>, filter: Bson?): List<String> =
        ranges.mapIndexed { index, (min, max) ->
            // 桶之间首尾相接，所以除最后一个用闭区间外都取 [min_i, min_{i+1})；
            // 最后一个必须包含最大值本身，否则 _id 最大的那个文档会被漏掉
            val last = index == ranges.lastIndex
            IdRange(min, if (last) max else ranges[index + 1].first, last)
        }.map { toJson(combine(filter, it)) }

    private fun autoPartitionNum(total: Long): Int =
        ceil(total.toDouble() / DOCS_PER_PARTITION).toInt().coerceIn(1, MAX_PARTITIONS)

    /** `$bucketAuto` 按 `_id` 把文档尽量均匀地分到 [buckets] 个桶里，每个桶给出 `{min, max}`。 */
    private fun bucketBounds(
        collection: MongoCollection<Document>,
        filter: Bson?,
        buckets: Int,
    ): List<Pair<Any?, Any?>> {
        val stages = buildList {
            if (filter != null) {
                add(Document("\$match", filter.toBsonDocument(BsonDocument::class.java, CODECS)))
            }
            add(Document("\$bucketAuto", Document("groupBy", "\$_id").append("buckets", buckets)))
        }
        return collection.aggregate(stages, Document::class.java)
            .allowDiskUse(true)
            .map { it["_id"] as Document }
            .map { it["min"] to it["max"] }
            .toList()
    }

    private data class IdRange(val min: Any?, val max: Any?, val inclusiveMax: Boolean)

    private fun combine(filter: Bson?, range: IdRange): BsonDocument {
        val idCondition = Document().apply {
            if (range.min != null) append("\$gte", range.min)
            if (range.max != null) append(if (range.inclusiveMax) "\$lte" else "\$lt", range.max)
        }
        val conditions = buildList {
            if (filter != null) add(filter.toBsonDocument(BsonDocument::class.java, CODECS))
            if (idCondition.isNotEmpty()) add(Document("_id", idCondition).toBsonDocument(BsonDocument::class.java, CODECS))
        }
        return when (conditions.size) {
            0 -> BsonDocument()
            1 -> conditions.single()
            else -> Document("\$and", conditions).toBsonDocument(BsonDocument::class.java, CODECS)
        }
    }

    fun toJson(filter: Bson): String =
        filter.toBsonDocument(BsonDocument::class.java, CODECS).toJson(JSON_SETTINGS)

    fun parse(json: String): BsonDocument = BsonDocument.parse(json)

    private val CODECS = MongoClientSettings.getDefaultCodecRegistry()
}
