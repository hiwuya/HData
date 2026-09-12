package me.jayer.hdata.core.spi

/**
 * A machine-readable declaration of what a connector actually guarantees after a failure and restart.
 *
 * This exists because delivery semantics — at-least-once vs. exactly-once, whether a source can resume
 * instead of replaying from scratch, whether a sink needs an idempotency key to be safe under retry,
 * whether row order survives a restart — are currently scattered across prose in `docs/connectors.md` and
 * are not something a pipeline author can query or a build can validate. [TransformProvider.deliveryCapabilities]
 * lets a provider declare its own contract; [PipelineGraph] surfaces the resolved contract for the whole
 * pipeline in `--dryRun` output (see `docs/MATURITY_ASSESSMENT.md`, critical gap #2).
 *
 * A `null` return from [TransformProvider.deliveryCapabilities] (the default) means the provider has not
 * declared a contract yet, not that it has none — declaring one for every connector is ongoing work, and an
 * absent declaration must never be printed or read as an implicit guarantee.
 */
data class DeliveryCapabilities(
    /** The strongest delivery mode this connector actually guarantees end-to-end, not merely attempts. */
    val deliveryMode: DeliveryMode,
    /** How a *source* behaves across a restart. Always [ReplayBehavior.NOT_APPLICABLE] for a sink. */
    val replayBehavior: ReplayBehavior,
    /** The strongest ordering guarantee this connector preserves across a restart. */
    val ordering: OrderingScope,
    /**
     * For a sink: does it need an idempotency/dedup key from upstream to be safe under retry (a replayed
     * upstream record must not double-apply)? Always `false` for a source, where it does not apply.
     */
    val requiresIdempotencyKey: Boolean = false,
    /** A short, specific elaboration — e.g. what bounds a duplicate window, or what "resumable" excludes. */
    val notes: String? = null,
) {
    /** One-line rendering for `--dryRun` / log output. */
    fun describe(): String = buildString {
        append("delivery=").append(deliveryMode)
        append(", replay=").append(replayBehavior)
        append(", order=").append(ordering)
        if (requiresIdempotencyKey) append(", requires_idempotency_key")
        if (notes != null) append(" (").append(notes).append(')')
    }
}

/** The strongest delivery guarantee a connector actually provides end-to-end after a restart. */
enum class DeliveryMode {
    /** A restart can lose records the previous run had not yet made durable downstream. */
    AT_MOST_ONCE,

    /** A restart can re-deliver records the previous run already made durable downstream, never lose them. */
    AT_LEAST_ONCE,

    /** A restart neither loses nor duplicates a record's effect (typically via an idempotent/transactional write). */
    EXACTLY_ONCE,
}

/** How a source's position across a restart relates to what it already emitted. */
enum class ReplayBehavior {
    /** Not a source (e.g. a sink), or the connector has no restart-relevant position at all (one-shot bounded read). */
    NOT_APPLICABLE,

    /** A restart re-reads from the beginning; every previously emitted record is a duplicate. */
    FULL_REPLAY,

    /** A restart resumes from a persisted position; only a bounded window since the last checkpoint can duplicate. */
    RESUMABLE,
}

/** The strongest ordering guarantee a connector preserves, including across a restart. */
enum class OrderingScope {
    /** No ordering guarantee at all. */
    NONE,

    /** Order is preserved only among records sharing the same key/partition, not globally. */
    PER_KEY,

    /** A single total order is preserved across the whole connector (e.g. one log/table scanned single-threaded). */
    GLOBAL,
}
