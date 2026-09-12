# Connector contract suite

This suite defines the evidence required before a connector/runner/mode cell can be called
**qualified**. It is intentionally stricter than a compilation check or a successful dry run.

## Required evidence

| Contract | Required assertion |
|---|---|
| Read/write round trip | A real service receives and returns representative rows where the connector supports both directions. |
| Schema and nulls | Declared scalar types and null values survive the supported storage format without coercion or loss. |
| Retry and delivery | The provider's `DeliveryCapabilities` matches a failure/retry test, including the required idempotency boundary. |
| Dead letter | A failing row retains its original row, timestamp, window, and the real exception when dead-letter handling is supported. |
| Serialization | Every remote-executed DoFn/transform has `SerializableUtils.ensureSerializable` coverage. |
| Recovery | An unbounded or position-bearing source proves its documented restart behavior against a real service. |
| Secret redaction | Logs, dry-run output, and run manifests never serialize credentials or arbitrary connector configuration. |

Not every row applies to every connector: a read-only snapshot connector has no write round trip, and a
connector with no durable cursor cannot qualify its recovery row until that behavior exists. Such a gap
keeps the cell experimental or logic-tested-only; it must never be waived by a prose claim.

## Running and retaining evidence

Run the repository's manually triggered **Connector Contract Qualification** workflow once per module.
It runs ordinary tests followed by the `integration-tests` Testcontainers profile and uploads the XML
reports. The workflow is deliberately `workflow_dispatch` only.

Locally, run the equivalent commands:

```bash
mvn -B -pl hdata-jdbc -am test -DskipITs
mvn -B -Pintegration-tests -pl hdata-jdbc -am test
```

Record the workflow run URL, commit SHA, runner, module, and report artifact before promoting a matrix
cell in [RUNNER_SUPPORT.md](RUNNER_SUPPORT.md).
