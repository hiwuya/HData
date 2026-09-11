package me.jayer.hdata.redis

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.redis.transform.RedisWriteFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `WriteToRedis` writes Redis values with `set`, `lpush`, `rpush`, `sadd`, or `hset`, with dead-letter support.
 *
 * @author wuya
 */
class RedisWriteProvider : TypedTransformProvider<RedisWriteConfig>(RedisWriteConfig::class.java) {

    override fun identifier(): String = "WriteToRedis"

    override fun description(): String = "Write Redis values (set / lpush / rpush / sadd / hset)"

    override fun outputCollectionNames(): List<String> = listOf(Tags.ERROR_OUTPUT)

    override fun create(config: RedisWriteConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return RedisSink(config, context.errorHandling != null, context.transformName)
    }
}

private class RedisSink(
    private val config: RedisWriteConfig,
    private val deadLetter: Boolean,
    private val transformName: String,
) : RowSink() {

    override fun write(input: PCollection<Row>): PCollection<Row>? {
        val errorSchema = ErrorSchemas.of(input.schema)
        val errors = input
            .apply("Write", ParDo.of(RedisWriteFn(config, errorSchema, deadLetter, transformName)))
            .setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
