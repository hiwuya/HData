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
 * Splits a collection into several non-overlapping `_id` ranges.
 *
 * The pre-refactor splitting used `sort(_id).skip(n).limit(m)`, which had two serious problems:
 *  1. **Every partition had to rescan from the start**: reading the k-th partition required MongoDB
 *     to first skip the first k·m documents, so the total cost was O(n²) and more partitions made
 *     it slower than not partitioning at all;
 *  2. **It was not a stable split**: as long as there was an insert or delete between `countDocuments()`
 *     and each partition's actual execution, the partition boundaries would shift as a whole, so some
 *     documents were read twice and some were never read, while the job still exited successfully.
 *
 * Now we first use `$bucketAuto` to compute the `_id` bucket boundaries, then translate them into
 * `_id >= a AND _id < b` filter conditions — every partition uses the `_id` index, and the boundaries
 * are concrete values unaffected by concurrent writes. This is also the splitting approach used by
 * Beam's built-in `MongoDbIO.BoundedMongoDbSource`.
 *
 * @author wuya
 */
internal object MongoBuckets {

    private val LOGGER = LoggerFactory.getLogger(MongoBuckets::class.java)

    /** Target number of documents per partition, used to estimate the bucket count when partition_num is not explicitly specified. */
    private const val DOCS_PER_PARTITION = 100_000L

    private const val MAX_PARTITIONS = 1000

    /** Extended-mode JSON: types like ObjectId / Long / Decimal128 remain their original type after a round trip. */
    private val JSON_SETTINGS: JsonWriterSettings =
        JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build()

    /**
     * @return The complete filter for each partition (extended JSON), with the user's [filter] and
     *         the `_id` range already combined with AND. Returns an empty list when the collection is empty.
     */
    fun partitionFilters(
        collection: MongoCollection<Document>,
        filter: Bson?,
        partitionNum: Int?,
    ): List<String> {
        val total = if (filter == null) collection.countDocuments() else collection.countDocuments(filter)
        if (total <= 0) {
            LOGGER.info("Collection [{}] has no matching documents", collection.namespace)
            return emptyList()
        }
        val buckets = partitionNum ?: autoPartitionNum(total)
        if (buckets <= 1) {
            return listOf(toJson(filter ?: BsonDocument()))
        }

        val bounds = bucketBounds(collection, filter, buckets)
        if (bounds.size <= 1) {
            LOGGER.info("Collection [{}] cannot be split into multiple buckets, falling back to single-partition read", collection.namespace)
            return listOf(toJson(filter ?: BsonDocument()))
        }

        LOGGER.info("Collection [{}] has {} documents, split into {} partitions", collection.namespace, total, bounds.size)
        return rangesToFilters(bounds, filter)
    }

    /**
     * Translates the `(min, max)` list produced by `$bucketAuto` into non-overlapping filters.
     *
     * Split into a standalone function so it can be unit-tested without MongoDB — getting the
     * partition boundaries wrong means missing or duplicated data, and neither failure makes the
     * job fail, so it can only be guarded by tests.
     */
    fun rangesToFilters(ranges: List<Pair<Any?, Any?>>, filter: Bson?): List<String> =
        ranges.mapIndexed { index, (min, max) ->
            // Buckets are contiguous end-to-end, so all but the last take [min_i, min_{i+1});
            // the last one must include the maximum value itself, otherwise the document with the largest _id is skipped
            val last = index == ranges.lastIndex
            IdRange(min, if (last) max else ranges[index + 1].first, last)
        }.map { toJson(combine(filter, it)) }

    private fun autoPartitionNum(total: Long): Int =
        ceil(total.toDouble() / DOCS_PER_PARTITION).toInt().coerceIn(1, MAX_PARTITIONS)

    /** `$bucketAuto` distributes documents as evenly as possible into [buckets] buckets by `_id`, giving `{min, max}` for each bucket. */
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
