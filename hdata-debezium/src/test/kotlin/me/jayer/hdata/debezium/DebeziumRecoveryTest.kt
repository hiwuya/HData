package me.jayer.hdata.debezium

import me.jayer.hdata.debezium.internal.DebeziumRecords
import me.jayer.hdata.debezium.transform.DebeziumReadFn
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.junit.jupiter.api.Test
import tools.jackson.databind.ObjectMapper
import java.nio.file.Files
import kotlin.test.assertEquals

/**
 * Proves a restart against the same persisted offset file actually resumes rather than replaying: this is the
 * scenario critical gap #1 in docs/MATURITY_ASSESSMENT.md calls out as untested. `SimpleSourceConnector` is
 * Debezium's own connector for exactly this: on `start()` it reads `context.offsetStorageReader().offset(partition)`
 * and skips every id at or below the persisted offset, so a second run against the same offset file must pick up
 * where the first left off, not from the beginning.
 */
private val recoveryTestMapper = ObjectMapper()

private fun idsOf(rows: Iterable<org.apache.beam.sdk.values.Row>): List<Int> =
    rows.map { row -> recoveryTestMapper.readTree(row.getString("key")).get("id").asInt() }.sorted()

class DebeziumRecoveryTest {

    private fun config(offsetFile: String, maxRecords: Int) = DebeziumReadConfig(
        connector = "simple",
        connectorClass = "io.debezium.connector.simple.SimpleSourceConnector",
        name = "recovery-job",
        offsetFile = offsetFile,
        maxRecords = maxRecords,
        extra = mapOf(
            "topic.name" to "simple-recovery",
            "record.count.per.batch" to "1",
            "batch.count" to "5",
            "include.timestamp" to "false",
            "offset.flush.interval.ms" to "100",
        ),
    )

    @Test
    fun `a restart against the same offset file resumes instead of replaying`() {
        val offset = Files.createTempFile("recovery-off", ".dat").toString()

        val firstPipeline = Pipeline.create()
        val firstOut = firstPipeline.apply(Create.of(listOf("")))
            .apply(ParDo.of(DebeziumReadFn(config(offset, maxRecords = 2))))
            .setRowSchema(DebeziumRecords.SCHEMA)
        PAssert.that(firstOut).satisfies { rows ->
            assertEquals(listOf(1, 2), idsOf(rows), "the first run must capture the first two records")
            null
        }
        firstPipeline.run()

        // Simulates the job restarting (a new worker, a new DoFn instance) against the same persisted offset
        // file: it must resume from id 3, never re-emit ids 1/2 that the first run already committed.
        val secondPipeline = Pipeline.create()
        val secondOut = secondPipeline.apply(Create.of(listOf("")))
            .apply(ParDo.of(DebeziumReadFn(config(offset, maxRecords = 3))))
            .setRowSchema(DebeziumRecords.SCHEMA)
        PAssert.that(secondOut).satisfies { rows ->
            assertEquals(
                listOf(3, 4, 5),
                idsOf(rows),
                "a restart against the same offset file must resume from the last committed id, not replay it",
            )
            null
        }
        secondPipeline.run()
    }
}
