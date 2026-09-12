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
mvn -Pintegration-tests -pl hdata-debezium test -Dtest=DebeziumMySqlContainerIT
```

The repository intentionally has only manually triggered workflows. Add Flink and Spark smoke commands
and retain their reports before changing any matrix cell to qualified.
