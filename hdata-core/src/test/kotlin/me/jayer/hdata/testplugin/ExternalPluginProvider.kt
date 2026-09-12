package me.jayer.hdata.testplugin

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TransformProvider
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/** Compiled into a temporary JAR by [me.jayer.hdata.core.plugin.PluginManagerTest]. */
class ExternalPluginProvider : TransformProvider {
    override fun identifier(): String = "ExternalPlugin"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun from(config: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> = ExternalPluginSource()
}

class ExternalPluginSource : RowSource() {
    override fun read(begin: PBegin): PCollection<Row> {
        val schema = Schema.builder().addStringField("origin").build()
        val row = Row.withSchema(schema).addValue("isolated-plugin").build()
        return begin.apply(Create.of(row).withRowSchema(schema))
    }
}
