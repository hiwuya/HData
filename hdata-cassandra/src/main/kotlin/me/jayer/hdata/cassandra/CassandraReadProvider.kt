package me.jayer.hdata.cassandra

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.cassandra.transform.CassandraReadFn
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `ReadFromCassandra`: executes a CQL SELECT query against a Cassandra cluster and streams the
 * result rows. The output schema is derived from the result set metadata at runtime.
 *
 * @author wuya
 */
class CassandraReadProvider : TypedTransformProvider<CassandraReadConfig>(CassandraReadConfig::class.java) {

    override fun identifier(): String = "ReadFromCassandra"

    override fun description(): String = "Read from Cassandra by executing a CQL SELECT query"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(
        config: CassandraReadConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return CassandraSource(config)
    }
}

private class CassandraSource(private val config: CassandraReadConfig) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        // Schema is derived at runtime from CQL result metadata.
        return begin
            .apply("Trigger", Create.of(listOf(1)))
            .apply("ReadFromCassandra", ParDo.of(CassandraReadFn(config)))
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
