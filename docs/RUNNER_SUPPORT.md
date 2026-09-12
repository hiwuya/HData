# Runner support matrix

This matrix records evidence present in this repository on 2026-09-12. A runner profile compiling is
not evidence that a connector works on a remote runner. No matrix cell is **qualified** yet.

| Connector group | DirectRunner batch | DirectRunner streaming | FlinkRunner | SparkRunner |
|---|---|---|---|---|
| JDBC, Kafka, Debezium, Filesystem, Hive, Redis, Iceberg | Real-service integration tests exist; the seven listed modules were also verified locally with Testcontainers. | Kafka and Debezium have connector-level streaming coverage; Debezium has a MySQL restart test. | Not qualified; no repeatable remote smoke test. | Not qualified; no repeatable remote smoke test. |
| Cassandra, ClickHouse, DynamoDB, Elasticsearch 6/8, MongoDB, Neo4j, Prometheus, Pulsar, RabbitMQ, SQS | Unit/DirectRunner tests and a Testcontainers integration test exist. They have not been accepted as a runner qualification set. | Only sources whose configuration is unbounded should be treated as streaming; each needs a runner-specific checkpoint/recovery test. | Not qualified. | Not qualified. |
| FTP, HBase | In-process or logic-level tests exist. | Not applicable until a durable streaming contract is introduced. | Not qualified. | Not qualified. |

## What qualifies a cell

A connector/runner/mode cell becomes **qualified** only after a repeatable command demonstrates its
read/write contract, null/schema handling, serialization, dead letter behavior where supported,
restart/replay behavior where relevant, and secret redaction. For a remote runner, the command must
also stage an isolated plugin and exercise worker-side execution.

Run the existing checks manually:

```bash
# Ordinary self-contained tests
mvn -B verify

# A selected real-service integration test
tools/test-integration.sh -pl hdata-debezium test -Dtest=DebeziumMySqlContainerIT
```

The manually triggered **Runner Profile Verification** workflow compiles and runs the ordinary suite
with either `-Pflink-runner` or `-Pspark-runner`. It is dependency/build compatibility evidence only;
it does not qualify a remote runner cell.

The repository intentionally has only manually triggered workflows. Add Flink and Spark smoke commands
and retain their reports before changing any matrix cell to qualified.

## Manual streaming plugin smoke jobs

[`hdata-runner-smoke-plugin`](../hdata-runner-smoke-plugin) is a separately packaged descriptor
plugin that supplies `ReadRunnerSmokeTicker`, an intentionally unbounded `GenerateSequence` source.
It gives runner checks a minimal streaming source that has no external service dependency while
requiring plugin discovery, artifact staging, worker-side deserialization, and cancellation.

Build the project/runtime classpath with the corresponding Maven profile, then invoke one of the
scripts below against a disposable runner. Each command writes a redacted run manifest; retain the
manifest, runner job URL, and runner logs with the manual workflow or release evidence. Confirm that
the job is streaming, emits `tick` rows, and cancel it after observation.

```bash
# FLINK_MASTER and HDATA_CLASSPATH are required; HDATA_CLASSPATH includes core, plugin API,
# resolved runtime dependencies, and the Flink runner from a -Pflink-runner build.
FLINK_MASTER=jobmanager.example:8081 \
HDATA_CLASSPATH="$(cat /tmp/hdata-runtime.cp)" \
deploy/runner-smoke/flink.sh

# Spark submits through the cluster's spark-submit. Set the packaged core JAR and resolved runtime jars.
SPARK_MASTER=spark://master.example:7077 \
HDATA_APP_JAR=hdata-core/target/hdata-core-1.0.0.jar \
HDATA_EXTRA_JARS="$(cat /tmp/hdata-runtime-jars.csv)" \
deploy/runner-smoke/spark.sh
```

These smoke assets make runner staging testable, but no report has yet been retained for either
runner. They therefore do **not** change any matrix cell to qualified.
