package me.jayer.hdata.mongodb

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.mongodb.transform.MongoReadFn
import me.jayer.hdata.mongodb.transform.MongoReadSplit
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory

/**
 * `ReadFromMongoDb`：按集合读取，使用 Splittable DoFn 按 `_id` 排序后的文档序号区间并行读。
 */
class MongoReadProvider : TypedTransformProvider<MongoReadConfig>(MongoReadConfig::class.java) {

    override fun identifier(): String = "ReadFromMongoDb"

    override fun description(): String = "按集合从 MongoDB 读取，使用 Splittable DoFn 并行读"

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
        val schema = buildSchema(config.schemaFields)
        LOGGER.info("ReadFromMongoDb 读取 {}.{}，schema={}", config.database, config.collection, schema)
        val split = MongoReadSplit(config.connectionUri, config.database, config.collection)
        return begin.apply("Collections", Create.of(listOf(split)))
            .apply(
                "Read",
                ParDo.of(
                    MongoReadFn(
                        config.connectionUri,
                        config.database,
                        config.collection,
                        config.schemaFields,
                        config.fetchSize,
                    )
                ),
            )
            .setRowSchema(schema)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

private val LOGGER = LoggerFactory.getLogger(MongoReadProvider::class.java)
