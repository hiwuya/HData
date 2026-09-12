package me.jayer.hdata.debezium.internal

import me.jayer.hdata.core.exception.HDataException
import org.junit.jupiter.api.Test
import java.nio.file.Files
import kotlin.test.assertFailsWith

/**
 * [JobIdentity] guards against a config mistake pointing a different job at an `offset_file` some other job
 * already owns — a case [OffsetLease] alone cannot catch once the original job has stopped running and its
 * lease has expired or been released, since the file would otherwise look simply "available".
 */
class JobIdentityTest {

    private fun offsetPath() = Files.createTempFile("identity-test-off", ".dat").toString()

    @Test
    fun `does nothing when job_id is not set`() {
        val offset = offsetPath()
        JobIdentity.checkOrRecord(offset, jobId = null)
        JobIdentity.checkOrRecord(offset, jobId = null) // repeatable, still a no-op
    }

    @Test
    fun `records job_id on first use and accepts the same job_id afterward`() {
        val offset = offsetPath()
        JobIdentity.checkOrRecord(offset, jobId = "orders-cdc")
        JobIdentity.checkOrRecord(offset, jobId = "orders-cdc") // a normal restart of the same job
    }

    @Test
    fun `rejects a different job_id against the same offset_file`() {
        val offset = offsetPath()
        JobIdentity.checkOrRecord(offset, jobId = "orders-cdc")
        val error = assertFailsWith<HDataException> {
            JobIdentity.checkOrRecord(offset, jobId = "customers-cdc")
        }
        error.message!!.let {
            assert(it.contains("orders-cdc")) { "should name the previously recorded job_id: $it" }
            assert(it.contains("customers-cdc")) { "should name the conflicting job_id: $it" }
        }
    }

    @Test
    fun `an offset_file with no recorded identity accepts any job_id`() {
        // Simulates job_id being adopted on an offset_file that predates this feature (or was never given one).
        val offset = offsetPath()
        JobIdentity.checkOrRecord(offset, jobId = "first-job-id-ever-seen")
    }
}
