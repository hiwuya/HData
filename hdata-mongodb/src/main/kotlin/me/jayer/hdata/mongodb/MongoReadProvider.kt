package me.jayer.hdata.mongodb

import com.mongodb.client.MongoClients
import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.mongodb.MongoAggregateCombineFn
import me.jayer.hdata.mongodb.MongoAggregateToRowFn
import me.jayer.hdata.mongodb.aggregateSchema
import me.jayer.hdata.mongodb.internal.MongoBuckets
import me.jayer.hdata.mongodb.transform.MongoPartialAggregateFn
import me.jayer.hdata.mongodb.transform.MongoReadFn
import me.jayer.hdata.mongodb.transform.MongoReadSplit
import org.apache.beam.sdk.coders.SerializableCoder
import org.apache.beam.sdk.transforms.Combine
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.bson.BsonDocument
import org.bson.Document
import org.slf4j.LoggerFactory

/**
 * `ReadFromMongoDb`：按 `_id` 区间并行读集合，用 Splittable DoFn 逐个分片认领。
 *
 * 分片边界在构图阶段用 `$bucketAuto` 求出，所以提交作业的机器需要能连上 MongoDB。
 *
 * @author wuya
 */
class MongoReadProvider : TypedTransformProvider<MongoReadConfig>(MongoReadConfig::class.java) {

    override fun identifier(): String = "ReadFromMongoDb"

    override fun description(): String = "按 _id 区间并行读 MongoDB 集合，使用 Splittable DoFn"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(
        config: MongoReadConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return MongoSource(config)
    }
}

private class MongoSource(private val config: MongoReadConfig) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        val codec = MongoRowCodec.of(config.schemaFields)
        // 聚合下推：按 _id 分片做局部聚合，再全局归并——既真下推到 MongoDB，又能跨分片得到正确全局结果。
        if (config.aggregate.isNotEmpty()) {
            val schema = aggregateSchema(config.aggregate)
            // 正常按分片并行；空集合（partitionFilters 返回空）时退化为单分片过滤，保证产出 count=0 的那一行。
            val filters = partitionFilters(config.partitionNum).ifEmpty { listOf(config.filter.ifBlank { "{}" }) }
            val split = MongoReadSplit(config.database, config.collection, filters)
            LOGGER.info("ReadFromMongoDb {}.{} 聚合下推（{} 个分片做局部聚合，最终全局归并）", config.database, config.collection, filters.size)
            val partials = begin.apply("Splits", Create.of(split))
                .apply("PartialAggregate", ParDo.of(MongoPartialAggregateFn(config.connectionUri, config.aggregate)))
            partials.setCoder(SerializableCoder.of(me.jayer.hdata.mongodb.PartialAgg::class.java))
            val merged = partials.apply("MergeAggregate", Combine.globally(MongoAggregateCombineFn(config.aggregate)))
            merged.setCoder(SerializableCoder.of(me.jayer.hdata.mongodb.PartialAgg::class.java))
            return merged.apply("ToRow", ParDo.of(MongoAggregateToRowFn(config.aggregate, schema))).setRowSchema(schema)
        }

        // LIMIT 必须是全局的：分片读会把它变成"每片 LIMIT"，所以限行数时强制单分片，
        // 让 find().limit() 对整个结果集生效。
        val effectivePartitionNum = if (config.limit > 0) 1 else config.partitionNum
        val filters = partitionFilters(effectivePartitionNum)
        LOGGER.info("ReadFromMongoDb {}.{} 切成 {} 个分片", config.database, config.collection, filters.size)

        if (filters.isEmpty()) {
            return begin.apply("Empty", Create.empty(codec.schema)).setRowSchema(codec.schema)
        }

        val split = MongoReadSplit(config.database, config.collection, filters)
        return begin.apply("Splits", Create.of(split))
            .apply("Read", ParDo.of(MongoReadFn(config.connectionUri, codec, config.fetchSize, config.limit)))
            .setRowSchema(codec.schema)
    }

    private fun partitionFilters(effectivePartitionNum: Int?): List<String> =
        MongoClients.create(config.connectionUri).use { client ->
            val collection = client.getDatabase(config.database)
                .getCollection(config.collection, Document::class.java)
            val filter = config.filter.takeIf { it.isNotBlank() }?.let { BsonDocument.parse(it) }
            MongoBuckets.partitionFilters(collection, filter, effectivePartitionNum)
        }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

private val LOGGER = LoggerFactory.getLogger(MongoReadProvider::class.java)
