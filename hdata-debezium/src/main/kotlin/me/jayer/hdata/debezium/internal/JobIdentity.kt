package me.jayer.hdata.debezium.internal

import me.jayer.hdata.core.exception.HDataException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Records and checks an optional stable identity (`job_id`) for a `ReadFromDebezium` job, independent of
 * `offset_file`'s literal path. `OffsetLease` already stops two *concurrently running* instances from
 * advancing the same offset file, but it has nothing to say about a config mistake that points an unrelated
 * job at a file some other job already owns once that job isn't running (e.g. a copy-pasted pipeline config,
 * or a shared storage path reused by accident) — the file would simply look "available" and get silently
 * adopted. `job_id` catches that: unlike the lease, this record is written once and never cleared, so it
 * survives across restarts and even across the owning job being stopped for a while.
 */
internal object JobIdentity {

    /**
     * If `jobId` is null, does nothing (identity checking is opt-in). Otherwise records `jobId` the first
     * time `offsetFilePath` is used, or verifies it matches on every later call; throws [HDataException] on a
     * mismatch.
     */
    fun checkOrRecord(offsetFilePath: String, jobId: String?) {
        if (jobId == null) return
        val identityPath = Paths.get("$offsetFilePath.identity")
        val existing = if (Files.exists(identityPath)) {
            Files.readString(identityPath, StandardCharsets.UTF_8).trim()
        } else {
            null
        }
        if (existing == null) {
            identityPath.parent?.let { Files.createDirectories(it) }
            Files.writeString(identityPath, jobId, StandardCharsets.UTF_8)
            return
        }
        if (existing != jobId) {
            throw HDataException(
                "job_id mismatch for offset state at $offsetFilePath: this run declares job_id=\"$jobId\", " +
                    "but $identityPath already records job_id=\"$existing\". Pointing a different job at the " +
                    "same offset_file will corrupt or duplicate its captured change stream; use a different " +
                    "offset_file for this job, or delete $identityPath if that prior job's state truly belongs " +
                    "to this one."
            )
        }
    }
}
