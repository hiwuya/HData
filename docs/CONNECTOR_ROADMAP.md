# Connector roadmap

This roadmap records which connector families are a good fit for HData and why. The list is intentionally selective: a connector is added only when its delivery semantics, schema model, and local integration-test story fit Beam and HData.

## Current coverage

HData already includes JDBC, Kafka, Pulsar, Hive, Iceberg, Debezium, Redis, Neo4j, MongoDB, HBase, FTP, filesystem, and Elasticsearch 6/8. These cover relational databases, files and tables, CDC, queues, key-value stores, graph data, and document search.

## Next priorities

| Priority | Connector | Direction | Reason | Main risk |
|---|---|---|---|---|
| P1 | RabbitMQ | Read / write | Common queue workload, clear acknowledgement model, and a lightweight broker image is available for repeatable integration tests. | Delivery guarantees and redelivery must be explicit; bounded reads need a snapshot or message-count limit. |
| P1 | ClickHouse | Read / write | Columnar analytics is a distinct workload from JDBC; the native HTTP or binary client can support batch writes and parallel reads. | Type mapping, insert retries, and partition-aware reads need careful design. |
| P2 | Amazon SQS | Read / write | Useful cloud queue source and sink with a local emulator for deterministic tests. | At-least-once delivery and visibility timeouts must be surfaced in configuration. |
| P2 | Prometheus | Read | Metrics export is useful for operational pipelines and can be queried over HTTP without a heavyweight database. | Time-series labels do not map naturally to a fixed Beam row schema. |
| P3 | DynamoDB | Read / write | A document store with a local emulator and a meaningful demand profile. | Scan pagination, consistent reads, and nested attribute conversion add substantial surface area. |

RabbitMQ should be implemented first, followed by ClickHouse. SQS and DynamoDB should remain behind those two until their retry and bounded-read semantics are specified. Cloud-only systems, notification APIs, and vendor-specific SaaS connectors are intentionally deferred because they make local tests and reproducible builds harder without adding a new core data model.

## Design gate for a new connector

Before adding a module, document its bounded or streaming read model, retry and acknowledgement behavior, schema conversion rules, dead-letter behavior, and serialization boundary. The module must have at least one real service test in the integration-test profile when a lightweight container or emulator exists. Pure logic tests remain useful, but they do not replace a protocol-level test.

