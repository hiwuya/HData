# Connector roadmap

This roadmap records which connector families are a good fit for HData and why. The list is intentionally selective: a connector is added only when its delivery semantics, schema model, and local integration-test story fit Beam and HData.

## Current coverage

HData already includes JDBC, Kafka, Pulsar, Hive, Iceberg, Debezium, Redis, Neo4j, MongoDB, HBase, FTP, filesystem, Elasticsearch 6/8, RabbitMQ, ClickHouse, Cassandra, Amazon SQS, Prometheus, and DynamoDB. These cover relational databases, columnar analytics, wide-column stores, files and tables, CDC, queues, key-value stores, graph data, document search, metrics, and cloud-native storage.

## Testcontainers coverage

Every connector should have a real-service integration test in the `integration-tests` Maven profile when a lightweight,
version-compatible image exists; a logic-only test suite does not substitute for it (a mocked ES 6.x client and a mocked
Debezium `SourceRecord` both passed for a long time while the real write/read path was broken — see the git history around
the ES6 `doc_type` and Debezium `include.schema.changes` fixes). Current status:

* Covered by a real-service container test: JDBC (PostgreSQL), Kafka, Pulsar, Redis, MongoDB, Elasticsearch 6/8, Neo4j,
  Filesystem (MinIO/S3A), Debezium (MySQL binlog), Iceberg (MinIO/S3A warehouse), Hive (metastore Thrift service),
  RabbitMQ, ClickHouse, Cassandra, Amazon SQS (LocalStack), Prometheus (self-scrape), DynamoDB (amazon/dynamodb-local).
* FTP: deferred. Apache FtpServer already runs as a real (not mocked) FTP protocol implementation in-process
  (`EmbeddedFtpServer`), exercising the actual wire protocol (`REST`/`STOR`/`APPE`/`RNFR-RNTO`) that
  `WriteToFtp`/`ReadFromFtp` depend on. A container test would mainly add coverage of a *different* server's `LIST`
  output quirks; the common images for that (vsftpd-based) need passive-mode port-range/address configuration that is
  genuinely fragile under Docker/rootless-Podman networking. Revisit only if a specific server-compatibility bug shows up.
* HBase: deferred. No official Apache HBase Docker image exists; the community images that are still maintained top out
  around HBase 2.1, while this project's client is 2.6.1 — a real version/protocol-compatibility risk, not just an
  inconvenience. Revisit if an official or actively maintained 2.6.x-compatible image becomes available.

The regular `mvn test` suite remains self-contained; container tests are supplemental, run only under `-Pintegration-tests`.

## Next priorities

The last roadmap review (P1–P3) is fully shipped: RabbitMQ and ClickHouse (P1), Cassandra, Amazon SQS and
Prometheus (P2), DynamoDB (P3) — each with config validation, logic-layer tests, and a real-service
Testcontainers test (see "Testcontainers coverage" above). There is no open priority list right now;
new candidates go through the design gate below before a table like this gets refilled.

Cloud-only systems, notification/SaaS APIs (issue trackers,
chat/email delivery, document/spreadsheet APIs), and connectors that just add another dialect over a protocol HData
already speaks (another JDBC-compatible database, another CDC source, another object-store API) are intentionally
deferred: JDBC and Debezium already generalize across dialects, cloud-only services require paid accounts that break
reproducible local tests, and SaaS/notification APIs are business-system integration rather than a bounded/streaming
data pipeline with a clear delivery model. A vector database (e.g. a single-node-image, upsert-based store) is also left
off this list for now — "search by vector similarity" does not fit the existing bounded-snapshot/streaming read model, so
it needs a new read abstraction before it needs a container image.

## Design gate for a new connector

Before adding a module, document its bounded or streaming read model, retry and acknowledgement behavior, schema conversion rules, dead-letter behavior, and serialization boundary. The module must have at least one real service test in the integration-test profile when a lightweight container or emulator exists. Pure logic tests remain useful, but they do not replace a protocol-level test.

