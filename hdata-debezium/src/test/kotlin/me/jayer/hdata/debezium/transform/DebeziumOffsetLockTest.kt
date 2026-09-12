package me.jayer.hdata.debezium.transform

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.debezium.DebeziumReadConfig
import org.junit.jupiter.api.Test
import java.nio.file.Files
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * `ReadFromDebezium` runs unbounded CDC against a persistent offset file; if two instances ever point at the
 * same file (an orphaned worker from a prior deployment, a misconfigured duplicate job, a botched restart),
 * their commits interleave and can corrupt recovery. [DebeziumReadFn] guards against that with a same-host
 * [java.nio.channels.FileLock] on `<offsetFile>.lock`, taken in `@Setup` and released in `@Teardown`.
 */
class DebeziumOffsetLockTest {

    private fun simpleConfig(name: String, offsetFile: String, topic: String) = DebeziumReadConfig(
        connector = "simple",
        connectorClass = "io.debezium.connector.simple.SimpleSourceConnector",
        name = name,
        offsetFile = offsetFile,
        maxRecords = 1,
        extra = mapOf(
            "topic.name" to topic,
            "record.count.per.batch" to "1",
            "batch.count" to "1",
            "include.timestamp" to "false",
            "offset.flush.interval.ms" to "1000",
        ),
    )

    @Test
    fun `a second reader on the same offset file is rejected while the first is running`() {
        val offset = Files.createTempFile("lock-test-off", ".dat").toString()
        val first = DebeziumReadFn(simpleConfig("first", offset, "lock-topic-1"))
        first.setup()
        try {
            val second = DebeziumReadFn(simpleConfig("second", offset, "lock-topic-2"))
            val error = assertFailsWith<HDataException> { second.setup() }
            assertTrue(error.message!!.contains(offset), "error should name the contended offset file")
            // setup() failed before an engine/lock was assigned to `second`, so this is a harmless no-op,
            // exercising the same defensive path a real pipeline cancellation would hit.
            second.teardown()
        } finally {
            first.teardown()
        }
    }

    @Test
    fun `the lock is released once the first reader tears down, so a restart can proceed`() {
        val offset = Files.createTempFile("lock-test-off2", ".dat").toString()
        val first = DebeziumReadFn(simpleConfig("first", offset, "lock-topic-3"))
        first.setup()
        first.teardown()

        // Simulates a job restart reusing the same persisted offset file: must succeed now that the prior
        // owner has released it.
        val second = DebeziumReadFn(simpleConfig("second", offset, "lock-topic-4"))
        second.setup()
        second.teardown()
    }
}
