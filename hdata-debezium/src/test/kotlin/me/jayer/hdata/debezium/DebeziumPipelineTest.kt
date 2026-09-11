package me.jayer.hdata.debezium

import me.jayer.hdata.debezium.internal.DebeziumRecords
import me.jayer.hdata.debezium.transform.DebeziumReadFn
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * End-to-end: really starts the embedded engine with Debezium's own database-free `SimpleSourceConnector`, verifying
 * the whole chain of "engine start → Consumer receives events → queue → DoFn output". `max_records=1` makes the DoFn
 * stop the engine as soon as the first record arrives, avoiding long idle spinning in the database-free scenario where
 * the connector does not exit on its own.
 * Real MySQL/Postgres binlog/WAL capture cannot be verified in this environment; that part is left for users to verify
 * against a production database.
 */
class DebeziumPipelineTest {

    @Test
    fun `SimpleSourceConnector 端到端跑通`() {
        val config = DebeziumReadConfig(
            connector = "simple",
            connectorClass = "io.debezium.connector.simple.SimpleSourceConnector",
            name = "test",
            maxRecords = 3,
            extra = mapOf(
                "topic.name" to "simple",
                "record.count.per.batch" to "3",
                "batch.count" to "1",
                "include.timestamp" to "false",
                "offset.flush.interval.ms" to "1000",
            ),
        )
        val p = Pipeline.create()
        val trigger = p.apply(Create.of(listOf("")))
        val out = trigger.apply(ParDo.of(DebeziumReadFn(config))).setRowSchema(DebeziumRecords.SCHEMA)
        PAssert.that(out).satisfies { rows ->
            assertTrue(rows.toList().size >= 1)
            null
        }
        p.run()
    }

    @Test
    fun `max_records 真的限定输出条数`() {
        // max_records used to count the records the engine thread put into the queue, not the records already output:
        // when the limit was hit there was often still a batch pending in the queue, and the cleanup dumped them all
        // out, so the actual output exceeded what was declared.
        val config = DebeziumReadConfig(
            connector = "simple",
            connectorClass = "io.debezium.connector.simple.SimpleSourceConnector",
            name = "test-max",
            maxRecords = 2,
            extra = mapOf(
                "topic.name" to "simple-max",
                "record.count.per.batch" to "3",
                "batch.count" to "1",
                "include.timestamp" to "false",
                "offset.flush.interval.ms" to "1000",
            ),
        )
        val p = Pipeline.create()
        val trigger = p.apply(Create.of(listOf("")))
        val out = trigger.apply(ParDo.of(DebeziumReadFn(config))).setRowSchema(DebeziumRecords.SCHEMA)
        PAssert.that(out).satisfies { rows ->
            val list = rows.toList()
            assertEquals(2, list.size, "with max_records=2 the output must be exactly 2 records, no more")
            null
        }
        p.run()
    }
}
