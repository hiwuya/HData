package me.jayer.hdata.mongodb

import com.mongodb.client.MongoClients
import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.core.spi.DeliveryCapabilities
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.OrderingScope
import me.jayer.hdata.core.spi.ReplayBehavior
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
 * `ReadFromMongoDb`: reads a collection in parallel by `_id` range, claiming each partition one at a time with a Splittable DoFn.
 *
 * Partition boundaries are computed at graph-construction time using `$bucketAuto`, so the machine that submits the job must be able to connect to MongoDB.
 *
 * @author wuya
 */
class MongoReadProvider : TypedTransformProvider<MongoReadConfig>(MongoReadConfig::class.java) {

    override fun identifier(): String = "ReadFromMongoDb"

    override fun description(): String = "Read a MongoDB collection in parallel by _id range, using a Splittable DoFn"

    override fun supportTier(): ConnectorSupportTier = ConnectorSupportTier.EXPERIMENTAL

    override fun deliveryCapabilities(config: TransformConfig): DeliveryCapabilities = DeliveryCapabilities(
        deliveryMode = DeliveryMode.AT_LEAST_ONCE, replayBehavior = ReplayBehavior.FULL_REPLAY,
        ordering = OrderingScope.NONE,
        notes = "Bounded _id-range snapshot with no persisted position; a restart repeats matching documents and parallel buckets have no global order.",
    )

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
        // Push-down aggregation: do a partial aggregation per `_id` partition, then merge globally — this truly pushes
        // down to MongoDB while still yielding the correct global result across partitions.
        if (config.aggregate.isNotEmpty()) {
            val schema = aggregateSchema(config.aggregate)
            // Normally runs in parallel per partition; when the collection is empty (partitionFilters returns empty) it
            // falls back to a single-partition filter, guaranteeing the row with count=0 is still produced.
            val filters = partitionFilters(config.partitionNum).ifEmpty { listOf(config.filter.ifBlank { "{}" }) }
            val split = MongoReadSplit(config.database, config.collection, filters)
            LOGGER.info("ReadFromMongoDb {}.{} push-down aggregation ({} partitions do partial aggregation, then global merge)", config.database, config.collection, filters.size)
            val partials = begin.apply("Splits", Create.of(split))
                .apply("PartialAggregate", ParDo.of(MongoPartialAggregateFn(config.connectionUri, config.aggregate)))
            partials.setCoder(SerializableCoder.of(me.jayer.hdata.mongodb.PartialAgg::class.java))
            val merged = partials.apply("MergeAggregate", Combine.globally(MongoAggregateCombineFn(config.aggregate)))
            merged.setCoder(SerializableCoder.of(me.jayer.hdata.mongodb.PartialAgg::class.java))
            return merged.apply("ToRow", ParDo.of(MongoAggregateToRowFn(config.aggregate, schema))).setRowSchema(schema)
        }

        // LIMIT must be global: partitioned reads would turn it into "LIMIT per partition", so when a row limit
        // is set we force a single partition, letting find().limit() apply to the whole result set.
        val effectivePartitionNum = if (config.limit > 0) 1 else config.partitionNum
        val filters = partitionFilters(effectivePartitionNum)
        LOGGER.info("ReadFromMongoDb {}.{} split into {} partitions", config.database, config.collection, filters.size)

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
