package me.jayer.hdata.debezium.internal

import me.jayer.hdata.core.exception.HDataException
import org.junit.jupiter.api.Test
import java.nio.file.Files
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * [OffsetLease] replaced an earlier OS-[java.nio.channels.FileLock]-based design that depended on `@Teardown`
 * to release it — unreliable, since Beam's DoFn contract does not guarantee `@Teardown` runs at all. These
 * tests exercise the lease's actual safety property: a fresh lease blocks a second acquire, a stale one
 * (timeout elapsed, no heartbeat) can be taken over without any explicit release, and an owner that lost its
 * lease to a takeover finds out on its next heartbeat.
 */
class OffsetLeaseTest {

    private fun offsetPath() = Files.createTempFile("lease-test-off", ".dat").toString()

    @Test
    fun `a fresh lease blocks a second acquire`() {
        val offset = offsetPath()
        OffsetLease.acquire(offset, leaseTimeoutMs = 10_000)
        val error = assertFailsWith<HDataException> { OffsetLease.acquire(offset, leaseTimeoutMs = 10_000) }
        assertTrue(error.message!!.contains("already leased"))
    }

    @Test
    fun `a stale lease can be taken over without any explicit release`() {
        val offset = offsetPath()
        OffsetLease.acquire(offset, leaseTimeoutMs = 20)
        Thread.sleep(60)
        // No release() call anywhere: this must succeed purely because the timeout elapsed, proving ownership
        // does not depend on the previous owner (or its @Teardown) doing anything.
        OffsetLease.acquire(offset, leaseTimeoutMs = 20)
    }

    @Test
    fun `heartbeat keeps a lease fresh so a second acquire keeps failing`() {
        val offset = offsetPath()
        val lease = OffsetLease.acquire(offset, leaseTimeoutMs = 200)
        Thread.sleep(120)
        lease.heartbeat()
        Thread.sleep(120)
        // 240ms have passed in total, more than the 200ms timeout, but the heartbeat at the 120ms mark reset
        // the clock, so the lease should still be fresh (only ~120ms since the last heartbeat).
        val error = assertFailsWith<HDataException> { OffsetLease.acquire(offset, leaseTimeoutMs = 200) }
        assertTrue(error.message!!.contains("already leased"))
    }

    @Test
    fun `an owner that lost its lease to a takeover discovers it on the next heartbeat`() {
        val offset = offsetPath()
        val first = OffsetLease.acquire(offset, leaseTimeoutMs = 20)
        Thread.sleep(60)
        OffsetLease.acquire(offset, leaseTimeoutMs = 20) // takes over; `first` is now stale

        val error = assertFailsWith<HDataException> { first.heartbeat() }
        assertTrue(error.message!!.contains("Lost the offset-state lease"))
    }

    @Test
    fun `release lets a fresh lease be reacquired immediately`() {
        val offset = offsetPath()
        val lease = OffsetLease.acquire(offset, leaseTimeoutMs = 10_000)
        lease.release()
        // No sleep: without release() this would fail immediately, since the lease is nowhere near stale.
        OffsetLease.acquire(offset, leaseTimeoutMs = 10_000)
    }

    @Test
    fun `release is a no-op once another owner has taken over`() {
        // Both acquisitions use the same timeout, as two instances of the same job (same configured
        // lease_timeout_ms) would: staleness is judged against the acquirer's own configured timeout, not a
        // value baked into the lease file, so a real restart always uses the same window as the original owner.
        val offset = offsetPath()
        val first = OffsetLease.acquire(offset, leaseTimeoutMs = 20)
        Thread.sleep(60)
        val second = OffsetLease.acquire(offset, leaseTimeoutMs = 20) // takes over; `first` is now stale

        first.release() // must not clear `second`'s still-fresh lease

        // A large timeout here so execution jitter since `second` acquired can never itself look stale — this
        // must fail purely because `second` still legitimately owns the lease, not from a timing coincidence.
        val error = assertFailsWith<HDataException> { OffsetLease.acquire(offset, leaseTimeoutMs = 10_000) }
        assertTrue(error.message!!.contains("already leased"))
        second.release()
    }
}
