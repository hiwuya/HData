package me.jayer.hdata.debezium.internal

import me.jayer.hdata.core.exception.HDataException
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.util.UUID

/**
 * A time-based lease that lets an unbounded `ReadFromDebezium` job claim ownership of a persistent offset
 * file without depending on any Beam DoFn lifecycle callback to release it.
 *
 * An earlier version of this connector held an OS [java.nio.channels.FileLock] on the offset file for the
 * whole job's lifetime, released in `@Teardown`. That was reverted: the Beam DoFn contract does not guarantee
 * `@Teardown` runs at all, and testing showed a completed pipeline run could still be holding the lock
 * afterward — which would leave a legitimate restart permanently unable to acquire state it owns. A lease
 * fixes this by expiring on its own: the owner must call [heartbeat] at least once every [leaseTimeoutMs], or
 * a new instance is free to take over, regardless of whether the previous owner ever released anything.
 *
 * The read-decide-write step that grants, renews, or releases the lease is itself guarded by a short-lived OS
 * file lock on a dedicated mutex file, held only for that one synchronous operation (not for the caller's
 * lifetime), so two processes racing to acquire or renew at the same instant cannot both succeed. If a
 * process dies mid-operation, the OS releases that brief lock automatically — this is a genuine improvement
 * over the reverted design, not merely a smaller window for the same problem, because ownership no longer
 * depends on any explicit release happening at all.
 *
 * This is still a same-host, same-filesystem mechanism (advisory `flock`-style locking for the mutex, plus a
 * plain file for the lease record) — it does not implement distributed consensus and cannot detect a former
 * owner that is still alive but has stopped heartbeating (e.g. a long GC pause); such an owner is expected to
 * discover the takeover on its next [heartbeat] call and stop.
 */
internal class OffsetLease private constructor(
    private val leasePath: Path,
    private val ownerId: String,
    private val leaseTimeoutMs: Long,
) {

    /** Renews the lease. Throws if another owner has since taken over because this owner's lease went stale. */
    fun heartbeat() {
        withMutex {
            val current = readRecord()
            if (current != null && current.ownerId != ownerId) {
                throw HDataException(
                    "Lost the offset-state lease for $leasePath to another owner (${current.ownerId}); this " +
                        "usually means this instance stopped heartbeating for more than ${leaseTimeoutMs}ms " +
                        "(a long pause, a stuck engine, or a slow shutdown) and a restart took over. Stopping " +
                        "now to avoid advancing the same offset state as the new owner."
                )
            }
            writeRecord(Record(ownerId, System.currentTimeMillis()))
        }
    }

    /** Best-effort: clears the lease if this instance still owns it, so a restart need not wait out the timeout. */
    fun release() {
        try {
            withMutex {
                val current = readRecord()
                if (current != null && current.ownerId == ownerId) {
                    Files.deleteIfExists(leasePath)
                }
            }
        } catch (_: Throwable) {
        }
    }

    private fun <T> withMutex(block: () -> T): T {
        val mutexPath = leasePath.resolveSibling("${leasePath.fileName}.mutex")
        mutexPath.parent?.let { Files.createDirectories(it) }
        FileChannel.open(mutexPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE).use { channel ->
            channel.lock().use {
                return block()
            }
        }
    }

    private fun readRecord(): Record? {
        if (!Files.exists(leasePath)) return null
        val lines = try {
            Files.readAllLines(leasePath, StandardCharsets.UTF_8)
        } catch (_: java.io.IOException) {
            return null
        }
        if (lines.size < 2) return null
        val heartbeatMs = lines[1].toLongOrNull() ?: return null
        return Record(lines[0], heartbeatMs)
    }

    private fun writeRecord(record: Record) {
        leasePath.parent?.let { Files.createDirectories(it) }
        val tmp = leasePath.resolveSibling("${leasePath.fileName}.tmp-$ownerId")
        Files.write(tmp, listOf(record.ownerId, record.heartbeatMs.toString()), StandardCharsets.UTF_8)
        try {
            Files.move(tmp, leasePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        } catch (_: AtomicMoveNotSupportedException) {
            Files.move(tmp, leasePath, StandardCopyOption.REPLACE_EXISTING)
        }
    }

    private data class Record(val ownerId: String, val heartbeatMs: Long)

    companion object {
        /**
         * Acquires the lease for `offsetFilePath`, taking over a stale one (no heartbeat within
         * [leaseTimeoutMs]) if present. Throws [HDataException] if another owner's lease is still fresh.
         */
        fun acquire(offsetFilePath: String, leaseTimeoutMs: Long): OffsetLease {
            val leasePath = Paths.get("$offsetFilePath.lease")
            val ownerId = UUID.randomUUID().toString()
            val lease = OffsetLease(leasePath, ownerId, leaseTimeoutMs)
            lease.withMutex {
                val current = lease.readRecord()
                val now = System.currentTimeMillis()
                if (current != null && now - current.heartbeatMs < leaseTimeoutMs) {
                    throw HDataException(
                        "Offset state at $offsetFilePath is already leased by another live instance (owner " +
                            "${current.ownerId}, last heartbeat ${now - current.heartbeatMs}ms ago, timeout " +
                            "${leaseTimeoutMs}ms). Two owners advancing the same offset/schema-history state " +
                            "can corrupt or duplicate the captured change stream; stop the other instance, " +
                            "wait for its lease to expire, or give this job its own " +
                            "offset_file/schema_history_file."
                    )
                }
                lease.writeRecord(Record(ownerId, now))
            }
            return lease
        }
    }
}
