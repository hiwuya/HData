# Maturity assessment

This assessment records the repository's current engineering maturity and the work required before
claiming general production readiness. It is based on the implementation, tests, documentation, and
build configuration in this repository on 2026-09-12. It does not claim measured throughput,
availability, or production incident history, none of which can be inferred from source code.

## Position

HData is an **engineering beta / pre-GA data integration runtime**. Its core is more mature than a
prototype: it has a typed graph model, strict configuration binding, bounded/unbounded source
selection, dead-letter propagation, runner profiles, connector isolation, and a substantial test
suite. It is not yet at the operational maturity of broadly deployed integration platforms because
state durability, delivery guarantees, release governance, and cross-runner evidence are incomplete.

The immediate goal should be reliable operation of a deliberately supported set of connectors, not a
larger connector catalog.

## Scoring rubric

| Score | Meaning |
|---:|---|
| 1 | Experimental: API and behavior can change without a compatibility promise. |
| 2 | Prototype: useful paths work, but production failure modes are not yet specified or tested. |
| 3 | Engineering beta: documented contracts and broad automated tests exist; production use needs explicit limits. |
| 4 | Production-ready: durability, compatibility, operations, and release controls are evidenced for supported paths. |
| 5 | Ecosystem-mature: stable extension ecosystem, compatibility policy, broad deployment evidence, and sustained maintenance. |

## Current scorecard

| Area | Score | Evidence in this repository | Gap to the next level |
|---|:---:|---|---|
| Core graph and configuration model | 3.5 | YAML parser, variable substitution, strict unknown-property rejection, DAG validation, windows, dead-letter wiring, and 152 core tests. | Publish compatibility guarantees for the pipeline format and plugin API; add migration rules. |
| Connector implementation quality | 3.0 | 20 source/sink modules, typed configs, explicit unsupported cases, SDF or official Beam IO paths, and connector-specific tests. | Define one quality contract per connector and mark unsupported operational modes clearly. |
| Batch/streaming and CDC | 2.0 | Source boundedness selects runner mode; Debezium, Kafka, RabbitMQ, and SQS expose streaming paths. | Make source state, checkpoint ownership, replay behavior, and delivery guarantees durable and testable. |
| Reliability semantics | 2.0 | Retries, dead letters, and some idempotent writes exist; unsupported Kafka exactly-once fails explicitly. | Provide end-to-end semantics per source/sink pair and recovery tests for process, worker, and network failure. |
| Test strategy | 3.0 | Pure logic, DirectRunner behavior tests, serialization tests, and optional Testcontainers integration tests cover many connectors. | Run a supported-runner matrix and recovery/scale scenarios automatically on a controlled schedule. |
| Operations and observability | 2.5 | Exit codes, dry run, Beam metrics, runner submission notes, logging, and deployment guidance exist. | Add a stable job identity, structured run summary, metric/alert catalogue, and executable deployment templates. |
| Extension and dependency isolation | 3.0 | `hdata-plugin-api`, descriptor validation, child-first class loading, and Beam artifact staging are implemented and tested on DirectRunner. | Version the API independently; publish plugin packaging, compatibility, checksum, and remote-runner test contracts. |
| Release and supply-chain governance | 1.5 | Maven build, NOTICE, security policy, issue/PR templates, and manual CI workflow are present. | Establish releases, tags, changelog, compatibility policy, SBOM, dependency/security scanning, and signed/reproducible artifacts. |
| Documentation and contributor experience | 3.0 | English and Chinese READMEs are aligned; connector, architecture, deployment, security, and contribution guides exist. | Add task-oriented production runbooks, connector support tiers, upgrade guides, and complete runnable examples. |

**Overall: 2.7 / 5.** The codebase is suitable for controlled pilots and development environments.
Production use should initially be limited to explicitly qualified connector/runner combinations with
persistent state configured and an owner responsible for recovery.

## What is already strong

### Correctness-oriented connector design

The repository consistently favors explicit failure over a configuration option that appears to work
but is ignored. Examples include strict config binding, connector validation, type mapping tests,
serialization checks, and explicit rejection of unsupported delivery modes. The SDF implementation
guidelines also preserve splitting and progress semantics rather than treating parallelism as a
cosmetic option.

### A useful execution foundation

The project uses the same Beam abstraction for bounded and unbounded inputs. This is the right
foundation for a unified batch/streaming runtime: the graph builder propagates source boundedness into
`StreamingOptions`, and execution mode can reject an impossible batch job before submission. Beam
itself makes runner portability a core goal, but documents that runner capabilities differ; HData
must therefore qualify each supported runner instead of assuming equivalence.

### Dependency conflict containment has started

The plugin API boundary, descriptor, child-first loader, duplicate detection, and artifact staging
address a real Java connector problem. This is a meaningful foundation, but it remains a runtime
loading mechanism rather than a complete plugin distribution and compatibility system.

### Tests use the right layers

The project combines unit tests, DirectRunner behavior tests, in-process service substitutes, and
optional Testcontainers tests. This matches the recommended distinction between focused transform
tests and tests against real datastores. The remaining step is to execute the same connector contract
on every runner that is advertised as supported.

## Critical gaps

### 1. CDC state is not production-safe by default (partially addressed)

`ReadFromDebezium` used to create temporary offset and schema-history files whenever `offset_file` or
`schema_history_file` was absent, silently discarding them on a restart or a different worker. Progress
so far:

- `DebeziumReadConfig.validate()` rejects an unbounded (streaming) job unless `offset_file` (and, for
  MySQL, `schema_history_file`) is set to a durable path, or `allow_ephemeral_state: true` explicitly
  accepts the risk for a development-only run.
- A real data-loss bug was found and fixed while adding a restart-recovery test: `DebeziumReadFn` used
  to build and start the embedded engine eagerly in `@Setup`. Beam's splittable-DoFn machinery also runs
  `@Setup` on a throwaway instance it creates purely to compute the initial restriction
  (`SplittableParDo$SplitRestrictionFn`), which never processes any element. With a persistent
  `offset_file`, that throwaway instance ran a real engine against the real source and — because
  Debezium commits offsets on a normal engine stop — silently advanced the persisted offset before the
  real processing instance ever ran, so a job could skip records it never emitted. The engine now starts
  lazily on the first `@ProcessElement` call instead.
- `DebeziumRecoveryTest` is a real (non-integration, in-process) recovery test: it runs `ReadFromDebezium`
  against `SimpleSourceConnector` (Debezium's own connector for exactly this, since it round-trips through
  `context.offsetStorageReader()`) with a persistent `offset_file`, asserts the first run emits exactly
  the records requested, then runs a second, separate pipeline against the same `offset_file` and asserts
  it resumes from the first run's last committed id rather than replaying or skipping it. This is the
  "recovery integration test" this gap originally called out as missing — it is a synthetic-connector,
  same-JVM test, not a real MySQL/Postgres binlog restart, but it does exercise the actual offset-commit
  and offset-read code path.

A first fix for a related but distinct problem — two job instances racing to advance the same
`offset_file` concurrently — was implemented as an exclusive same-host `FileLock`, released in
`@Teardown`, and then **reverted** after testing showed it does not hold up: the Beam DoFn contract does
not guarantee `@Teardown` runs at all, and in testing a completed pipeline run still held the lock after
`pipeline.run()` returned. A lock keyed to a lifecycle callback that may never fire is worse than no lock:
it can leave a legitimate restart permanently unable to acquire state it owns.

This is now replaced with `OffsetLease` (`me.jayer.hdata.debezium.internal.OffsetLease`), a time-based
lease recorded in `<offset_file>.lease`: the DoFn acquires it in `ensureStarted()` and renews it once per
`@ProcessElement` call; a second instance may take it over once `lease_timeout_ms` (default 30s,
configurable) has passed without a renewal, regardless of whether the previous owner released anything —
including a crash, a `kill -9`, or `@Teardown` simply never running. The read-decide-write step is itself
guarded by a short-lived OS file lock on a separate mutex file, held only for that one synchronous
operation rather than the job's lifetime, so it cannot reintroduce the same failure mode. `OffsetLeaseTest`
covers acquire/reject/takeover/heartbeat-loses-ownership/release directly; `DebeziumRecoveryTest`'s second
run exercises a real takeover through the DoFn.

Known limits of this mechanism, still open:

- it is same-host/same-filesystem (advisory `flock`-style for the mutex plus a plain file for the lease
  record), not distributed consensus — it does not protect two instances on different hosts or NFS
  mounts, or two different `offset_file` paths that happen to point at the same logical source;
- it cannot detect a former owner that is still alive but has stopped heartbeating (e.g. a long GC
  pause); that owner only discovers the takeover on its own next heartbeat and stops, so there is a
  window (bounded by `lease_timeout_ms`) where two owners could both believe they hold the lease;
- two different `offset_file` paths that happen to point at the same logical source are not detected
  (`job_id`, below, only catches the opposite: the same `offset_file` used by two different jobs).

`DebeziumMySqlContainerIT` now includes a real-MySQL recovery test (`a restart resumes streaming after a
completed snapshot, without re-snapshotting`, Testcontainers, `@Tag("integration")`): it snapshots two seed
rows with a persistent `offset_file`/`schema_history_file`, inserts two more rows, restarts against the
same files, and asserts the second run resumes streaming from the persisted binlog position — seeing only
the two new rows as insert events, not a re-snapshot of the originals. This closes the "real MySQL/Postgres
connector" item. **Verified**, using the rootless-Podman fallback `CONTRIBUTING.md` documents for
environments with no Docker daemon (Ryuk had to stay disabled — it hung against the rootless socket rather
than erroring, which is worth knowing if a run seems stuck rather than failed):
`mvn -q -Pintegration-tests -pl hdata-debezium test -Dtest=DebeziumMySqlContainerIT` → 2 run, 0 failures, 0
errors, 33.9s.

An optional `job_id` config field (`JobIdentity`, recorded permanently in `<offset_file>.identity`, never
cleared) now catches the config-drift case the lease cannot: once a job's lease has expired or been
released, its `offset_file` looks simply "available", so an unrelated job pointed at the same file by
mistake (a copied config, a reused storage path) would otherwise silently adopt its state. This is a
narrower guarantee than a full job-identity-addressed state store (state is still keyed by `offset_file`'s
path, `job_id` is a check layered on top, and it is opt-in) — but it is real, tested (`JobIdentityTest`),
and closes the specific misconfiguration this gap was concerned about.

Replay and duplicate behavior after a crash is now documented (`docs/connectors.md#debezium`, "Crash and
restart behavior"): at-least-once delivery, a duplicate window bounded by `offset.flush.interval.ms`, no
data loss, per-table/WAL ordering preserved across a restart, and the resulting sink-idempotency
requirement.

The reference CDC engine documents durable file, JDBC, Redis, and Kafka-backed offset stores and
separate schema history. Mature integration runtimes also make state locking and environment-specific
state configuration first-class concepts.

### 2. Delivery guarantees are connector-local instead of pipeline-visible (partially addressed)

At-least-once, retry, acknowledgement, deduplication, ordering, and overwrite semantics were described
only in prose, scattered across the connector reference, with no machine-readable contract a user could
query or a build could check.

`DeliveryCapabilities` (`hdata-plugin-api`, `me.jayer.hdata.core.spi`) is now a real, declarable contract:
`deliveryMode` (the strongest of at-most-once / at-least-once / exactly-once a connector actually
guarantees), `replayBehavior` (a source's restart behavior: not-applicable / full-replay / resumable),
`ordering` (none / per-key / global), and `requiresIdempotencyKey` (whether a sink needs upstream
deduplication to be safe under retry), plus a free-text `notes`. `TransformProvider.deliveryCapabilities(config)`
defaults to `null` ("not yet declared", never "guarantees nothing"); `PipelineGraphBuilder` resolves it per
node using the node's actual bound config (so a connector whose guarantee depends on its config, like
Kafka's `sink_delivery_guarantee` or Hive/Iceberg's `write_mode`, reports correctly), `GraphNode.describe()`
appends it to the line Beam already prints for every graph node, and `HData` logs which nodes have **no**
declared contract yet, both in `--dryRun` and before a real submission.

Every connector provider now declares a delivery contract. Core built-in transforms intentionally remain
undeclared because they neither acquire nor persist external delivery state.

Not yet done, and worth calling out explicitly:
- graph construction rejects a `FULL_REPLAY` source path into a sink that declares an idempotency-key
  requirement. There is currently no built-in deduplication transform, so ordinary intermediate transforms
  do not suppress this rejection;
- `--runManifest` records the resolved contract as JSON.

### 3. Runner support lacks qualification evidence

Flink and Spark profiles compile and have deployment guidance, but the repository has no repeatable
connector contract matrix showing which source/sink paths work on DirectRunner, Flink, and Spark. A
local DirectRunner test does not prove remote worker staging, checkpoint recovery, serialization, or
watermark behavior.

Keep the existing manually triggered workflow policy. Add separate manual workflows or profiles for:

1. DirectRunner unit/in-process verification on every requested run;
2. Testcontainers connector qualification;
3. a small Flink cluster smoke matrix;
4. a Spark local or cluster smoke matrix;
5. periodic recovery and compatibility tests for the supported streaming connectors.

The acceptance criterion is a published matrix, not a claim that all runners are interchangeable.

### 4. The plugin boundary needs a distribution contract

The current plugin descriptor has one API version and stages JARs to workers. Before third parties
rely on it, add artifact coordinates/version, host compatibility range, checksums, a declared private
dependency set, and a package validation command. Test a plugin-supplied connector on the remote
Flink and Spark paths, not only DirectRunner. Do not promise binary compatibility until this contract
is versioned independently of `hdata-core`.

### 5. Release and security evidence is insufficient

There are no release tags or changelog in the repository. Without them, users cannot identify a
supported version, assess breaking changes, or reproduce a deployment. The CI workflow is intentionally
manual, which is appropriate for the repository policy, but a release still needs recorded evidence.

A release candidate must require:

- `mvn -B verify` and the selected Testcontainers suite;
- a dependency vulnerability report and generated SBOM;
- a changelog and compatibility notes;
- signed or checksum-published Maven artifacts;
- a version tag and a retained build/test artifact;
- an explicit supported JDK, Maven, runner, and connector matrix.

### 6. Operations need a stable job-level interface

Beam metrics are available, and the deployment guide explains where each runner surfaces them.
`--runManifest=<path>` now writes an atomic, runner-neutral JSON record at validation, submission, and
terminal completion (where the caller waits). It includes the pipeline SHA-256 fingerprint, runner class,
job name, streaming mode, graph node types, declared support tiers, resolved delivery contracts, and
terminal state. It deliberately excludes all connector configuration and arbitrary runner options, because
either may contain credentials. Connector artifact versions, metric summaries, and a global job identity
remain open.

## Alignment plan

### Phase 0 — define and prevent unsafe operation

1. ✅ Require persistent CDC state for unbounded jobs, or require an explicit `allow_ephemeral_state: true`
   acknowledgement for development-only runs. (See critical gap #1.)
2. ✅ Add provider-level delivery capabilities and print the resolved pipeline contract in dry run. (See
   critical gap #2; validating incompatible combinations remains open.)
3. ✅ Create connector support tiers: **qualified**, **experimental**, and **logic-tested only**
   (`ConnectorSupportTier`, `hdata-plugin-api`). JDBC, Kafka, Debezium, Filesystem, Hive, Redis, Iceberg,
   Cassandra, ClickHouse, DynamoDB, Elasticsearch 6/8, MongoDB, Neo4j, Prometheus, Pulsar, RabbitMQ, and
   SQS declare **experimental** because each has a real-service Testcontainers test. The full Phase 1
   contract suite below does not exist yet, so nothing qualifies for **qualified**. FTP and HBase declare
   **logic-tested only** because their current in-process or logic-level coverage does not meet the
   real-service criterion.

   The "experimental" claim above was verified, not assumed: every one of these 7 connectors' Testcontainers
   `IT` tests was actually run (via the rootless-Podman fallback in `CONTRIBUTING.md`, `no_proxy` set for
   `localhost,127.0.0.1` — a configured HTTP proxy otherwise breaks the S3/MinIO client's calls to the
   container's mapped local port) — `PostgresJdbcIT`, `KafkaBrokerIT`, `DebeziumMySqlContainerIT` (both
   tests), `S3FilesystemIT`, `HiveMetastoreContainerIT`, `RedisContainerIT`, `IcebergMinioContainerIT`: 8 run,
   0 failures, 0 errors across the set.
4. Correct documentation drift immediately whenever the supported JDK, Maven, connector, or test claim changes.

Exit criterion: users can tell whether a job is safe to restart, can duplicate data, and is qualified
on their runner before it is submitted.

### Phase 1 — evidence the supported paths

1. ✅ Define one reusable connector contract suite: read/write round trip, schema/null handling, retry,
   dead letter, serialization, restart/replay, and secret redaction (`docs/CONNECTOR_CONTRACT.md`).
2. Run it against the qualified connectors with Testcontainers; retain logs and reports from manual CI runs.
3. ✅ Add Flink and Spark smoke jobs that include an isolated plugin and a streaming source
   (`hdata-runner-smoke-plugin`, `deploy/runner-smoke/`). Their execution reports remain required
   before any runner matrix cell is qualified.
4. ✅ Publish a runner × connector × mode matrix in the documentation (`docs/RUNNER_SUPPORT.md`).
5. ✅ Add deployment templates for a container image and a Flink submission path, including state
   and secret injection (`deploy/docker/Dockerfile`, `deploy/flink/job.yaml`, and `docs/DEPLOYMENT.md`).

Exit criterion: every qualified cell in the published matrix has a repeatable test command and an
owned recovery assertion.

### Phase 2 — establish a stable platform contract

1. ✅ Establish an independent plugin API compatibility policy and packaging tool
   (`docs/PLUGIN_API.md`, `tools/package-plugin.sh`). Publishing a signed external artifact remains
   a release-governance task below.
2. ✅ Add schema evolution rules and compatibility checks for pipeline files and connector configs
   (`format_version` / `config_version`, `docs/PIPELINE_COMPATIBILITY.md`).
3. ✅ Document stateful-transform admission requirements (`docs/STATEFUL_TRANSFORMS.md`). The current
   transform set is intentionally stateless; any future stateful transform must satisfy those requirements.
4. ✅ Add release evidence tooling and governance (`CHANGELOG.md`, `docs/UPGRADE.md`,
   `docs/DEPRECATION.md`, `tools/release-artifacts.sh`). Signing requires release-environment keys.

Exit criterion: a user can upgrade a connector or runtime with a documented compatibility decision and
can operate stateful/CDC jobs with a defined recovery outcome.

## Reference practices

The priorities above are derived from these primary references and adapted to HData's JVM/Beam design:

- [Apache Beam: testing pipelines](https://beam.apache.org/documentation/pipelines/test-your-pipeline/) describes transform, end-to-end, and runner testing layers.
- [Apache Beam: testing I/O transforms](https://beam.apache.org/documentation/io/testing/) separates datastore integration tests and recommends small and large-scale configurations.
- [Apache Beam: runner capability matrix](https://beam.apache.org/documentation/runners/capability-matrix/) documents that portability does not imply identical runner capability.
- [Debezium: state storage](https://debezium.io/documentation/reference/nightly/configuration/storage.html) documents persistent offsets and separate schema history required for restart recovery.
- [Meltano: state backends](https://docs.meltano.com/concepts/state_backends) documents persistent state and locking for concurrent runs.
- [Meltano: plugin locks](https://docs.meltano.com/concepts/plugins) documents version-controlled plugin definitions as a reproducibility mechanism.
