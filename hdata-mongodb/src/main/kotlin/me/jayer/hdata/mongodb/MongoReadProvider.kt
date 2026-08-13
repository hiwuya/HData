package me.jayer.hdata.mongodb

import com.mongodb.client.MongoClients
import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.mongodb.internal.MongoBuckets
import me.jayer.hdata.mongodb.transform.MongoReadFn
import me.jayer.hdata.mongodb.transform.MongoReadSplit
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
        val filters = partitionFilters()
        LOGGER.info("ReadFromMongoDb {}.{} 切成 {} 个分片", config.database, config.collection, filters.size)

        if (filters.isEmpty()) {
            return begin.apply("Empty", Create.empty(codec.schema)).setRowSchema(codec.schema)
        }

        val split = MongoReadSplit(config.database, config.collection, filters)
        return begin.apply("Splits", Create.of(split))
            .apply("Read", ParDo.of(MongoReadFn(config.connectionUri, codec, config.fetchSize)))
            .setRowSchema(codec.schema)
    }

    private fun partitionFilters(): List<String> =
        MongoClients.create(config.connectionUri).use { client ->
            val collection = client.getDatabase(config.database)
                .getCollection(config.collection, Document::class.java)
            val filter = config.filter.takeIf { it.isNotBlank() }?.let { BsonDocument.parse(it) }
            MongoBuckets.partitionFilters(collection, filter, config.partitionNum)
        }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

private val LOGGER = LoggerFactory.getLogger(MongoReadProvider::class.java)
