package me.jayer.hdata.core.spi

/**
 * A connector's declared support tier, per `docs/MATURITY_ASSESSMENT.md` Phase 0 item 3: "create connector
 * support tiers ... move a connector to qualified only after it meets the tests below."
 *
 * [TransformProvider.supportTier] defaults to `null`, meaning **not yet classified**, not "untested" — a
 * connector can be genuinely well-tested and simply not have had this method implemented yet. Do not read an
 * absent tier as a claim about quality in either direction.
 */
enum class ConnectorSupportTier {
    /**
     * Meets the full reusable connector contract suite planned in `docs/MATURITY_ASSESSMENT.md` Phase 1 —
     * read/write round trip, schema/null handling, retry, dead letter, serialization, restart/replay, and
     * secret redaction — evidenced by tests, and qualified on at least one non-Direct runner. As of this
     * writing that reusable suite does not exist yet as a formal, repeatable check, so no connector in this
     * repository currently qualifies for this tier; it exists so the distinction is meaningful once it does.
     */
    QUALIFIED,

    /**
     * Has a real-service integration test (e.g. Testcontainers) demonstrating actual read/write correctness
     * against a live instance of the target system, beyond unit/logic-level tests, but does not yet meet the
     * full contract above (commonly missing: an explicit restart/replay assertion, dead-letter coverage, or
     * qualification on a runner other than DirectRunner).
     */
    EXPERIMENTAL,

    /**
     * Only unit/logic-level tests exist (pure functions, DirectRunner behavior tests, serialization checks,
     * in-process fakes) — no test in this repository exercises this connector against a real instance of the
     * target system.
     */
    LOGIC_TESTED_ONLY,
}
