package me.jayer.hdata.clickhouse

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.clickhouse.transform.ClickHouseReadFn
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `ReadFromClickHouse`: executes a SQL query against ClickHouse and streams the result rows.
 *
 * The output schema is derived from the result set metadata at runtime, so it adapts automatically
 * to the query. The query **must** be a SELECT statement.
 *
 * Because the schema is not known at graph-construction time, the output PCollection does not
 * carry a compile-time schema. Downstream transforms that require a schema (e.g. `AssertEqual`)
 * should use the explicit-schema variant of this connector, or the pipeline should derive the
 * schema in a separate step.
 *
 * @author wuya
 */
class ClickHouseReadProvider : TypedTransformProvider<ClickHouseReadConfig>(ClickHouseReadConfig::class.java) {

    override fun identifier(): String = "ReadFromClickHouse"

    override fun description(): String = "Read from ClickHouse by executing a SQL query (schema derived at runtime)"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(
        config: ClickHouseReadConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return ClickHouseSource(config)
    }
}

private class ClickHouseSource(private val config: ClickHouseReadConfig) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        // The DoFn derives the output schema from the result set at runtime, so we cannot set a
        // compile-time schema on the PCollection. The Rows produced by the DoFn carry their own
        // schema. Downstream transforms that require a fixed schema (e.g. AssertEqual) should
        // use a schema-aware variant or derive the schema in a separate step.
        return begin
            .apply("Trigger", Create.of(listOf(1)))
            .apply("ReadFromClickHouse", ParDo.of(ClickHouseReadFn(config)))
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
