# Contributing to HData

Thanks for considering contributing to HData! This document covers how to set up the environment,
build and test, code conventions, and the submission flow.

## Environment requirements

- **JDK 17** (target bytecode Java 17; Kotlin `jvmTarget` is set to 17)
- **Maven 3.9+** (this project ships **no** Maven wrapper — use the system-installed `mvn`)
- First build needs network access; dependencies are pulled from Maven Central

> Kotlin 2.4.20 is used throughout the project.

## Build and test

```bash
# compile and package
mvn -q package

# run the unit and in-process test suite
mvn -q test

# run Testcontainers integration tests
mvn -q -Pintegration-tests verify

# CI-equivalent command for unit tests
mvn -B verify
```

End-to-end tests use in-process fakes and need no external database / message broker:

| Module | End-to-end approach |
|---|---|
| JDBC | H2 in-memory database |
| Hive | local temp dir + in-process `InMemoryHiveMetastore` |
| Kafka | Kafka's own `MockConsumer` / `MockProducer` (real Beam SDF) |
| FTP | Apache FtpServer in-process |
| Filesystem | local temp dir |
| Redis | embedded-redis |
| Iceberg | local temp dir + HadoopCatalog |
| Debezium | Debezium's `SimpleSourceConnector` |
| Neo4j / HBase / MongoDB / Elasticsearch | logic layer (codec / splitting / config validation) |

The fakes used per module and the invariants that must hold are documented in [AGENTS.md](AGENTS.md).

The integration-test profile adds real service tests whose classes end in `IT`. Tests use Testcontainers;
Podman is supported through its Docker-compatible socket. For rootless Podman, start the user socket and run:

```bash
systemctl --user enable --now podman.socket
DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock \
TESTCONTAINERS_RYUK_DISABLED=true \
mvn -q -Pintegration-tests verify
```

If a corporate proxy is configured, unset it for containers that expose a local endpoint so the client does
not send localhost traffic through the proxy.

## Code conventions

- **Package names**: always `me.jayer.hdata.*`.
- **Indentation**: Kotlin / Java use 4 spaces, UTF-8, LF line endings (see [.editorconfig](.editorconfig)).
- **Config keys**: `snake_case`; `FAIL_ON_UNKNOWN_PROPERTIES` is on by default, so adding a field must
  update the config class too.
- **Config options must actually take effect**: an added option must either be read by the
  implementation, or (when not yet supported) explicitly fail in `validate` — never silently degrade.
  Prefer adding a test that asserts the option really changes behavior.
- **Dead letter**: failed rows must go through `ErrorSchemas.failure(...)` and carry the original row's
  timestamp and window (`ValueInSingleWindow`); do not fabricate `Instant.now()` + `GlobalWindow`.
- **Resource lifecycle**: on the write path use `@Setup` / `@FinishBundle` / `@Teardown` to manage
  connection pools; failed rows go to the dead-letter stream. DoFns must not capture non-serializable objects.
- **Comment language**: code comments are in English; contributor-facing docs (this file, AGENTS, etc.)
  are in English as well.

## Adding a new connector

1. Implement `me.jayer.hdata.core.spi.TransformProvider` (or `TypedTransformProvider<C>`), one provider
   per `type` (read and write are separate), returning `RowSource` / `RowTransform` / `RowSink`.
2. Register it in `src/main/resources/META-INF/services/me.jayer.hdata.core.spi.TransformProvider`.
3. Add the new module to the root pom `<modules>` and depend on `hdata-core`.
4. Add at least one `SerializableUtils.ensureSerializable(...)` test confirming the DoFn is serializable.
5. When feasible, add an end-to-end test following the fakes table above.

## Submission flow

1. Fork the repo and branch off `main`.
2. Make sure `mvn -B verify` passes and add tests for behavior changes.
3. Write commit messages in English, using a concise imperative subject. Conventional Commit prefixes
   such as `feat:`, `fix:`, and `docs:` are encouraged for global readability.
4. Open a Pull Request and fill in the checklist in the PR template, linking the related Issue.
5. Wait for CI (GitHub Actions) to pass; a maintainer will review and merge.

## Issue / PR templates

Please use the templates provided in the repo when filing issues and PRs so we can triage quickly.
