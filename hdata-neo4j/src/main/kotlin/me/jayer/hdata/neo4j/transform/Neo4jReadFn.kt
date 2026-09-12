package me.jayer.hdata.neo4j.transform

import me.jayer.hdata.neo4j.Neo4jReadConfig
import me.jayer.hdata.neo4j.internal.DriverFactory
import me.jayer.hdata.neo4j.internal.RealDriverFactory
import me.jayer.hdata.neo4j.internal.newSession
import me.jayer.hdata.neo4j.internal.recordToRow
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.values.Row
import org.neo4j.driver.Driver

/**
 * Reads all Cypher-query results as a bounded snapshot and maps each record to a row through `schemaFields`.
 *
 * Each Cypher execution is one bounded SDF restriction. Each DoFn creates and closes its connection in setup and teardown; transient fields preserve serializability.
 * Tests can call [processElement] directly and create the connection lazily.
 *
 * @author wuya
 */
@DoFn.BoundedPerElement
class Neo4jReadFn(
    private val config: Neo4jReadConfig,
    private val schema: Schema,
    private val schemaFields: List<Pair<String, Schema.FieldType>>,
    private val driverFactory: DriverFactory = RealDriverFactory,
) : DoFn<String, Row>() {

    @Transient
    private var driver: Driver? = null

    @Setup
    fun setup() {
        driver = driverFactory.create(config)
    }

    @Teardown
    fun tearDown() {
        runCatching { driver?.close() }
        driver = null
    }

    @GetInitialRestriction
    fun getInitialRestriction(): OffsetRange = OffsetRange(0, 1)

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = OffsetRangeTracker(restriction)

    @ProcessElement
    fun processElement(
        @Element element: String,
        tracker: RestrictionTracker<OffsetRange, Long>,
        receiver: OutputReceiver<Row>,
    ) {
        if (!tracker.tryClaim(0)) return
        val d = driver ?: driverFactory.create(config).also { driver = it }
        newSession(d, config).use { session ->
            val result = session.run(config.query, config.parameters ?: emptyMap<String, Any>())
            while (result.hasNext()) {
                receiver.output(recordToRow(result.next(), schemaFields, schema))
            }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
