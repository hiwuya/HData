# Connector configuration parameter reference

This document lists **all** `Read*` / `Write*` configuration parameters (values, defaults, constraints/mutual-exclusions) per connector module.
All parameters are written under the pipeline node's `config:`.

Conventions:

- **Keys are `snake_case`**, bound by the framework to the Kotlin config class's properties via the SNAKE_CASE naming strategy;
  `FAIL_ON_UNKNOWN_PROPERTIES` is **on**, so a wrong parameter name or misspelled key errors directly rather than being silently ignored.
- **`error_handling` is stripped by the framework** and does not enter the config class; only sink connectors that support dead letter accept it (see the "Dead Letter" section at the end).
- **Variable substitution** happens before parsing: `${MYSQL_PASSWORD}` resolves from environment variable / `-D` system property / default
  (`${MYSQL_PASSWORD:-root}`); if absent with no default, graph construction fails.
- Booleans use `true` / `false`; lists use a YAML list or inline `[a, b]`; objects use a YAML mapping.

---

## Table of contents

- [Common: schema_fields type table](#schema_fields-type-table)
- [Core built-in transforms](#core-built-in-transforms)
- [JDBC](#jdbc)
- [Kafka](#kafka)
- [Pulsar](#pulsar)
- [Hive](#hive)
- [Redis](#redis)
- [Neo4j](#neo4j)
- [Iceberg](#iceberg)
- [Debezium](#debezium)
- [MongoDB](#mongodb)
- [HBase](#hbase)
- [FTP](#ftp)
- [Filesystem](#filesystem)
- [Elasticsearch 6](#elasticsearch-6)
- [Elasticsearch 8](#elasticsearch-8)
- [RabbitMQ](#rabbitmq)
- [ClickHouse](#clickhouse)
- [Cassandra](#cassandra)
- [Amazon SQS](#amazon-sqs)
- [Prometheus](#prometheus)
- [DynamoDB](#dynamodb)
- [Dead letter (error_handling)](#dead-letter-error_handling)

---

## schema_fields type table

Many connectors declare output/input columns with `schema_fields: ["name:TYPE", ...]`. Each connector's TYPE vocabulary differs:

| Connector | Accepted types (case-insensitive) |
|---|---|
| **JDBC** | no `schema_fields`; columns come from the source table metadata |
| **Hive** | no `schema_fields`; columns come from the metastore |
| **Debezium** | fixed schema (see its section), no `schema_fields` |
| **MongoDB** | `STRING` `INT32` `INT64` `DOUBLE` `BOOLEAN` `DATETIME` `BYTES` |
| **HBase** | `STRING` `INT32` `INT64` `DOUBLE` `BOOLEAN` `BYTES` |
| **Kafka** | format rather than type: `string` (UTF-8→STRING) / `raw` (as-is BYTES) |
| **Neo4j** | `STRING`; `INT64`/`BIGINT`/`LONG`; `INT32`/`INT`; `INT16`/`SMALLINT`/`SHORT`; `INT8`/`TINYINT`/`BYTE`; `FLOAT64`/`DOUBLE`; `FLOAT32`/`FLOAT`/`REAL`; `BOOLEAN`/`BOOL`; `BYTES`/`BINARY`/`BLOB` |
| **Iceberg** | `STRING`; `INT64`/`BIGINT`/`LONG`; `INT32`/`INT`; `FLOAT64`/`DOUBLE`; `FLOAT32`/`FLOAT`/`REAL`; `BOOLEAN`/`BOOL`; `BYTES`/`BINARY`/`BLOB`; `DATETIME`/`TIMESTAMP` |
| **Elasticsearch 6/8** | `STRING` `INT32` `INT64` `DOUBLE` `BOOLEAN` `DATETIME` `BYTES` |
| **FTP** | lowercase: `string` `int`/`int32` `long`/`int64` `float` `double` `boolean`/`bool` |
| **Filesystem** | lowercase: `string` `int`/`integer` `long` `float` `double` `boolean` `short` `byte` |

> When no type is written, Neo4j / Iceberg degrade to `STRING`; the degradation rules for MySQL/HBase/FTP/Filesystem are in their sections.
> When `schema_fields` is omitted, MongoDB / Elasticsearch degrade to a single `document` column (STRING, the whole record's JSON),
> which can be read out and written back directly.

---

## Core built-in transforms

These ship with `hdata-core`, are not external connectors, but are commonly used.

| type | parameter | type | default | description |
|---|---|---|---|---|
| `Create` | `elements` | list(object) | required (≥1) | create data from literals; schema inferred from literals; for testing/plumbing |
| `AssertEqual` | `elements` | list(object) | required | assert the input equals exactly the given record set (order-independent) and pass through unchanged; needs an assert-supporting runner like DirectRunner |
| `LogForTesting` | `level` | string | `INFO` | `TRACE`/`DEBUG`/`INFO`/`WARN`/`ERROR` |
| `LogForTesting` | `prefix` | string | `""` | prefix printed before each record |
| `MapToFields` | `fields` | map | `{}` | mapping of `target field: source field` (pure field reference, no expression) |
| `MapToFields` | `append` | bool | `false` | when `true`, keep all input fields then overlay `fields` |
| `MapToFields` | `drop` | list | `[]` | only effective when `append: true`, removes these names from the retained fields |
| `AddFields` | `fields` | map | required (non-empty) | appends typed JSON literal fields; existing field names are rejected |
| `Filter` | `field` | string | required | input field to inspect |
| `Filter` | `operator` | string | `equals` | one of `equals`, `not_equals`, `in`, `is_null`, `is_not_null` |
| `Filter` | `value` | JSON value | — | required for `equals` / `not_equals` and converted to the input field's type |
| `Filter` | `values` | list(JSON value) | `[]` | required and non-empty for `in`; every item is converted to the input field's type |
| `Explode` | `field` | string | required | ARRAY or ITERABLE field to expand |
| `Explode` | `output_field` | string | same as `field` | when different, retains the collection and appends each element under this field; otherwise replaces the collection field |
| `Flatten` | — | — | — | accepts no config; merges multiple same-schema inputs, at least one required |
| `StripErrorMetadata` | — | — | — | accepts no config; restores a dead-letter record to the original record; input must be a dead-letter stream |

`MapToFields` constraints: `fields` non-empty, or `append: true` and `drop` non-empty; `drop` must be used together with `append`.
`Filter` does not accept `value` or `values` with `is_null` / `is_not_null`.
`Explode` has inner-unnest behavior: null or empty collections produce no rows.

---

## JDBC

Read/write share connection fields (just write them at the `config` top level):

| parameter | type | default | description |
|---|---|---|---|
| `url` | string | required | JDBC URL, e.g. `jdbc:mysql://127.0.0.1:3306/demo` |
| `user` | string | `""` | |
| `password` | string | `""` | inject via `${MYSQL_PASSWORD}` recommended |
| `driver_class` | string | `""` | usually not needed, JDBC SPI auto-discovers the driver |
| `connection_properties` | map | `{}` | passed through to HikariCP, e.g. `maximumPoolSize` |

### ReadFromJdbc

| parameter | type | default | description / constraint |
|---|---|---|---|
| `tables` | list(string) | required* | table names, supports range syntax `t_order_${00-15}` |
| `query` | string | `""` | give SQL directly; once filled, `tables`/`where`/`partition_*` all become void |
| `columns` | list(string) | `["*"]` | column names, all columns by default |
| `where` | string | `""` | extra filter condition |
| `partition_column` | string | `""` | partition column; left empty auto-tries the primary key |
| `partition_num` | int? | `null` | number of shards; left empty estimates from the value range (cap 10000) |
| `fetch_size` | int | `10000` | how many rows per round trip |

\* At least one of `tables` and `query` is required, and they are mutually exclusive. In `query` mode `columns` must be `["*"]`.

### WriteToJdbc

| parameter | type | default | description |
|---|---|---|---|
| `table` | string | required | target table |
| `batch_size` | int | `10000` | how many rows to accumulate before committing |
| `retry_max_attempts` | int | `3` | max retries on batch failure |
| `retry_initial_seconds` | long | `3` | retry backoff base |
| `retry_max_seconds` | long | `60` | retry backoff cap (must be ≥ `retry_initial_seconds`) |

---

## Kafka

Key names aligned with the Flink Kafka connector. The read row's fixed schema is
`key` / `value` / `topic` / `partition` / `offset` / `timestamp` / `timestamp_type` / `headers`,
where the `key`/`value` types are decided by `key_format`/`value_format`.

### ReadFromKafka

| parameter | type | default | description / constraint |
|---|---|---|---|
| `bootstrap_servers` | string | required | comma-separated broker list |
| `topics` | list(string) | `[]` | topics to read; choose one of this and `topic_pattern` |
| `topic_pattern` | string | `""` | topic name regex; mutually exclusive with `topics` |
| `group_id` | string | `""` | needed by `group-offsets` mode and `commit_offsets_on_checkpoint` |
| `properties` | map | `{}` | passed through to KafkaConsumer, e.g. `security.protocol` |
| `scan_startup_mode` | string | `earliest-offset` | `earliest-offset`/`latest-offset`/`group-offsets`/`specific-offsets`/`timestamp` |
| `scan_startup_specific_offsets` | map | `{}` | for `scan_startup_mode=specific-offsets`, `"topic:partition" -> offset` |
| `scan_startup_timestamp_millis` | long? | `null` | for `scan_startup_mode=timestamp` |
| `scan_bounded_mode` | string | `latest-offset` | `unbounded`/`latest-offset`/`group-offsets`/`specific-offsets`/`timestamp`; **default `latest-offset` (bounded snapshot), write `unbounded` explicitly for streaming consumption** |
| `scan_bounded_specific_offsets` | map | `{}` | for `scan_bounded_mode=specific-offsets` (excluding that offset) |
| `scan_bounded_timestamp_millis` | long? | `null` | for `scan_bounded_mode=timestamp` |
| `key_format` | string | `string` | `string` (UTF-8→STRING) / `raw` (as-is BYTES) |
| `value_format` | string | `string` | same as above |
| `commit_offsets_on_checkpoint` | bool | `false` | commit offsets back to the consumer group after reading (monitoring only, does not affect HData fault tolerance); requires `group_id` |

`specific-offsets` / `timestamp` offset/timestamp parameters only take effect in the corresponding mode; configuring them in the wrong mode errors.

### WriteToKafka

| parameter | type | default | description / constraint |
|---|---|---|---|
| `bootstrap_servers` | string | required | |
| `topic` | string | `""` | target topic; **left empty routes by the input row's `topic` field** (can directly connect ReadFromKafka for cross-cluster migration) |
| `properties` | map | `{}` | passed through to KafkaProducer |
| `batch_size` | int | `1000` | how many records to flush and check send results (also the max in-flight count) |
| `key_format` | string | `string` | must match the input row `key` field type |
| `value_format` | string | `string` | must match the input row `value` field type |
| `sink_delivery_guarantee` | string | `at-least-once` | `at-least-once` (acks=all) / `none` (acks=0); **`exactly-once` errors explicitly**, not implemented |

The input row must contain a `value` field; `key` is optional.

---

## Pulsar

`ReadFromPulsar` reads a **bounded snapshot**, not a Splittable DoFn: one `DoFn` element opens one reader and drains
the topic up to `max_messages` (or until `receive_timeout_millis` passes with no new message), so a single element
does the whole read with no worker-level parallelism yet.

### ReadFromPulsar

| parameter | type | default | description / constraint |
|---|---|---|---|
| `service_url` | string | required | e.g. `pulsar://localhost:6650` |
| `topic` | string | required | |
| `start_position` | string | `earliest` | `earliest`/`latest` |
| `max_messages` | long | `-1` | max messages to read; `-1` means until the timeout below with no new message |
| `receive_timeout_millis` | long | `1000` | per-read timeout; used both as the read timeout and, with no `max_messages`, as the snapshot's stop signal |

Output schema: `topic`(STRING) + `value`(BYTES).

### WriteToPulsar

| parameter | type | default | description / constraint |
|---|---|---|---|
| `service_url` | string | required | |
| `topic` | string | required | |
| `value_field` | string | `value` | input row field written as the message body; `BYTES` as-is, other types via `toString()` |
| `batch_size` | int | `1000` | unused by the current synchronous-send path; reserved for a future batched producer |

A client and producer are created once per worker in `@Setup` and reused; failed rows go to the dead letter with
`ErrorSchemas.failure` and the original timestamp/window when `error_handling` is configured.

---

## Hive

It connects to the **metastore** (default 9083), not HiveServer2 (10000); metadata comes from the metastore,
and data is read/written directly to the files under the table directory, the same path as Trino's Hive connector. If the table does not exist, the write side errors directly (it will not create the table for you).

### ReadFromHive

| parameter | type | default | description / constraint |
|---|---|---|---|
| `metastore_uri` | string | required | e.g. `thrift://localhost:9083` |
| `database` | string | `default` | |
| `table` | string | required | |
| `partitions` | list(string) | `[]` | explicit partition names `dt=2024-01-01/hr=01`; mutually exclusive with `partition_filter` |
| `partition_filter` | string | `""` | metastore partition filter expression, e.g. `dt = '2024-01-01'` |
| `columns` | list(string) | `[]` | read only these columns (including partition columns); empty = all columns (less disk read under columnar formats) |
| `recursive_directories` | bool | `false` | whether to recurse when there are sub-directories under a partition directory |
| `hadoop_conf` | map | `{}` | passed through to Hadoop `Configuration`, e.g. `fs.defaultFS`, object-store ak/sk |
| `metastore_timeout_millis` | int | `60000` | metastore socket timeout |
| `split_bytes` | long | `67108864` (64MB) | max bytes per shard, only effective for splittable formats |

### WriteToHive

| parameter | type | default | description / constraint |
|---|---|---|---|
| `metastore_uri` | string | required | |
| `database` | string | `default` | |
| `table` | string | required | target table must already exist |
| `write_mode` | string | `append` | `append` (INSERT INTO) / `overwrite` (INSERT OVERWRITE, **clears only the partitions written this run**) |
| `create_partitions` | bool | `true` | register new partitions into the metastore after writing; off means Hive cannot see the new partitions |
| `num_shards` | int | `0` | write sharding count, `0` leaves it to the runner |
| `file_prefix` | string | `part` | file name prefix |
| `hadoop_conf` | map | `{}` | same as read side |
| `metastore_timeout_millis` | int | `60000` | |

Partitions are **dynamic**: each row decides its destination by its own partition-column value, and a single job can write an arbitrary number of partitions.

---

## Redis

Client uses Redisson. Connection fields (shared by read/write, written at the `config` top level):

| parameter | type | default | description |
|---|---|---|---|
| `host` | string | `localhost` | |
| `port` | int | `6379` | |
| `password` | string | `""` | |
| `database` | int | `0` | |
| `ssl` | bool | `false` | |
| `timeout_ms` | int | `5000` | connect/read/write timeout |

### ReadFromRedis

| parameter | type | default | description / constraint |
|---|---|---|---|
| `mode` | string | `scan` | `scan`/`keys`/`stream` |
| `key_pattern` | string | `*` | `mode=scan`: SCAN match pattern |
| `keys` | list(string) | `[]` | `mode=keys`: key list to read |
| `stream` | string | `""` | `mode=stream`: stream name to read |
| `start_id` | string | `-` | `mode=stream`: start entry id (`-`=head) |
| `end_id` | string | `+` | `mode=stream`: end entry id (`+`=tail) |

Mutual exclusion: `mode=scan` does not use `keys`/`stream`/`start_id`/`end_id`; `mode=keys` does not use `stream`/`start_id`/`end_id`;
`mode=stream` does not use `keys`/`key_pattern`. In `stream` mode each entry expands into multiple rows by field (`id`/`field`/`value`).

### WriteToRedis

| parameter | type | default | description / constraint |
|---|---|---|---|
| `mode` | string | `set` | `set`/`lpush`/`rpush`/`sadd`/`hset` |
| `key_field` | string | `key` | the row field used as the Redis key |
| `value_field` | string | `value` | the row field used as the value |
| `hash_field` | string | `field` | the row field used as the hash field name when `mode=hset` |
| `ttl_seconds` | long? | `null` | expiry in seconds; `set` uses SETEX, others use EXPIRE; no expiry if unset |

---

## Neo4j

Official Java Driver. Connection fields (shared by read/write):

| parameter | type | default | description |
|---|---|---|---|
| `uri` | string | `bolt://localhost:7687` | |
| `user` | string | `neo4j` | |
| `password` | string | `""` | |
| `database` | string? | `null` | leave empty for the default database |

### ReadFromNeo4j

| parameter | type | default | description / constraint |
|---|---|---|---|
| `query` | string | required | Cypher, returned records take fields by `schema_fields` |
| `schema_fields` | list(`name:TYPE`) | required | output schema; can be graphed without connecting to the database |
| `parameters` | map | `null` | constant query parameters `name -> value` |

### WriteToNeo4j

| parameter | type | default | description / constraint |
|---|---|---|---|
| `statement` | string | required | Cypher write statement, row fields bound to `$param` by name |
| `parameters` | map | `null` | `cypher param name -> row field name`; if empty, auto-bind by same name |
| `batch_size` | int | `1000` | one transaction per this many rows; on batch failure, fall back to per-row writes to locate bad data |

The keys of `parameters` must match exactly the `$xxx` placeholders in `statement`.

---

## Iceberg

HadoopCatalog, warehouse is a local directory or HDFS/S3 path. Shared by read/write:

| parameter | type | default | description |
|---|---|---|---|
| `warehouse` | string | required | catalog root directory; a local/HDFS path, or `s3a://bucket/prefix` with `hadoop_conf` below |
| `catalog_name` | string | `hdata` | |
| `table` | string | required | e.g. `db.users` |
| `schema_fields` | list(`name:TYPE`) | required | see the type table above (basic scalars, no nested/list/map) |
| `hadoop_conf` | map | `{}` | raw Hadoop `Configuration` overrides, e.g. `fs.s3a.endpoint` / `fs.s3a.access.key` for an S3-compatible warehouse. HadoopCatalog's commit relies on an atomic rename, which object stores do not natively provide — safe for a single writer, not for concurrent commits to the same table. |

### ReadFromIceberg

> Same shared parameters above + `schema_fields` (required). The output schema is declared by `schema_fields`; can be graphed without connecting to the database.

### WriteToIceberg

| parameter | type | default | description |
|---|---|---|---|
| `write_mode` | string | `append` | `append` (append) / `overwrite` (atomically clear the table **before all writes** then write) |

The table is auto-created if it does not exist (no partitions). `overwrite`'s table clear is guaranteed to happen exactly once and before the writes via a side input.

---

## Debezium

`ReadFromDebezium` does CDC based on the Debezium embedded engine, **read-only**. The output schema is fixed:

| field | meaning |
|---|---|
| `op` | `c`(insert)/`u`(update)/`d`(delete)/`r`(snapshot)/`t`(truncate) |
| `key` | primary key (JSON string) |
| `before` | the whole row before the change (JSON, may be null) |
| `after` | the whole row after the change (JSON, may be null) |
| `source` | source metadata (JSON) |
| `ts_ms` | change timestamp (milliseconds) |

Because the schema is fixed, the same pipeline can capture multiple tables of differing structures.

| parameter | type | default | description / constraint |
|---|---|---|---|
| `connector` | string | `mysql` | `mysql`/`postgres`, or give `connector_class` explicitly |
| `connector_class` | string? | `null` | any Debezium connector class, higher priority than `connector` |
| `host` | string? | `null` | required for built-in mysql/postgres |
| `port` | int? | `null` | 1..65535 |
| `user` | string? | `null` | required for built-in mysql/postgres |
| `password` | string? | `null` | |
| `database` | string? | `null` | required for postgres |
| `table_include` | string? | `null` | table name filter, e.g. `demo\\.t_user` |
| `snapshot_mode` | string? | `initial` | passed through to Debezium `snapshot.mode` |
| `server_name` | string? | `hdata` | corresponds to Debezium `topic.prefix` |
| `server_id` | int? | `184054` | for mysql |
| `offset_file` | string? | temp file | offset storage file |
| `schema_history_file` | string? | temp file | mysql schema history file |
| `name` | string? | `hdata-debezium` | engine name |
| `max_records` | int? | `null` | max records to capture (for testing) |
| `extra` | map | `null` | extra engine properties passed through, highest priority |

---

## MongoDB

Key names aligned with the Flink MongoDB connector. Client uses the official Java Driver. When `schema_fields` is omitted, it degrades to
a single `document` column (STRING, the whole document's extended JSON), which can be read out and written back directly.

### ReadFromMongoDb

| parameter | type | default | description / constraint |
|---|---|---|---|
| `connection_uri` | string | required | valid MongoDB URI |
| `database` | string | required | |
| `collection` | string | required | |
| `schema_fields` | list(`name:TYPE`) | `[]` | types see the table above; empty = single `document` column |
| `filter` | string | `""` | query condition (MongoDB query JSON), empty = full collection |
| `partition_num` | int? | `null` | parallel shard count 1..1000; left empty estimates from document count |
| `fetch_size` | int | `1000` | how many records per cursor round trip |

### WriteToMongoDb

| parameter | type | default | description / constraint |
|---|---|---|---|
| `connection_uri` | string | required | |
| `database` | string | required | |
| `collection` | string | required | |
| `schema_fields` | list(`name:TYPE`) | `[]` | empty = write by input `document`(STRING) fields |
| `upsert_keys` | list(string) | `[]` | overwrite-write by these fields; keys must be within `schema_fields`; empty = always insert (re-running produces duplicates) |
| `batch_size` | int | `1000` | how many rows to accumulate before committing |

---

## HBase

Scans reuse Beam's `HBaseIO.readAll()` (`read()` is the old `BoundedSource`, not used). HBase's type range is wider than Beam's,
and fixed-width types (INT32/INT64/DOUBLE/BOOLEAN) with mismatched cell widths error directly rather than silently truncating.

### ReadFromHBase

| parameter | type | default | description / constraint |
|---|---|---|---|
| `zookeeper_quorum` | string | required | e.g. `localhost:2181` |
| `zookeeper_znode_parent` | string | `""` | |
| `table` | string | required | |
| `rowkey_field` | string | `rowkey` | the field name for the row key in the output row |
| `rowkey_format` | string | `string` | `string`/`bytes`; **binary rowkey must use `bytes`, otherwise corrupted by UTF-8** |
| `family` | string | `cf` | entries in `schema_fields` without a column family default to this family |
| `schema_fields` | list | required | `qualifier:type` or `family:qualifier:type`; empty means pull all columns from the table, errors |
| `scan_start_row` | string | `""` | start rowkey (inclusive); empty = from the beginning |
| `scan_stop_row` | string | `""` | end rowkey (exclusive); empty = to the end |
| `scan_caching` | int | `100` | how many rows per RPC; full-table scan turns off block cache by default |
| `scan_cache_blocks` | bool | `false` | whether to enter RegionServer block cache; off by default for full-table scan |
| `properties` | map | `{}` | passed through to HBase properties |

### WriteToHBase

| parameter | type | default | description |
|---|---|---|---|
| `zookeeper_quorum` | string | required | |
| `zookeeper_znode_parent` | string | `""` | |
| `table` | string | required | |
| `rowkey_field` | string | `rowkey` | must match the input row's rowkey field type |
| `rowkey_format` | string | `string` | `string`/`bytes` |
| `family` | string | `cf` | |
| `schema_fields` | list | required | empty means each row writes only an empty Put |
| `batch_size` | int | `1000` | how many rows to accumulate before committing |
| `properties` | map | `{}` | |

---

## FTP

Reads in parallel **by byte range** (locating the start via `REST`), writes are sharded (each parallel unit produces `<file_prefix>-<shard number><extension>`).

### ReadFromFtp

| parameter | type | default | description / constraint |
|---|---|---|---|
| `host` / `host_name` | string | required | aliases, must not take different values |
| `port` | int | `21` | 1..65535 |
| `user` / `username` | string | `""` | alias |
| `password` | string | `""` | |
| `path` | string | required | remote directory or single file |
| `file_pattern` | string? | `null` | optional glob, e.g. `*.csv` |
| `file_format` | string | `text` | `text` (fixed single `content` column) / `csv` |
| `schema_fields` | list | `null` | required for `csv`; not available for `text` |
| `header` | bool | `false` | whether the `csv` first row is a header; not available for `text` |
| `encoding` | string | `UTF-8` | valid charset |
| `csv_delimiter` | string | `,` | single character; not available for `text` |
| `csv_quote` | string | `"` | single character; not available for `text` |
| `timeout_millis` | int | `30000` | connect/read/write timeout |

### WriteToFtp

| parameter | type | default | description |
|---|---|---|---|
| `host`/`host_name` | string | required | |
| `port` | int | `21` | |
| `user`/`username` | string | `""` | |
| `password` | string | `""` | |
| `path` | string | required | remote directory |
| `file_prefix` | string | `hdata-output` | file name prefix |
| `file_format` | string | `text` | `text`/`csv` |
| `schema_fields` | list | `null` | required for `csv` |
| `header` | bool | `false` | `csv` writes a header |
| `encoding` | string | `UTF-8` | |
| `csv_delimiter` | string | `,` | single character |
| `csv_quote` | string | `"` | single character |
| `batch_size` | int | `1000` | flush after accumulating this many rows |
| `timeout_millis` | int | `30000` | |

---

## Filesystem

Goes through Beam's `FileSystems`, with the filesystem decided by `path`'s scheme. `text` uses `TextIO.readFiles()` (true byte-range splitting).

### ReadFromFilesystem

| parameter | type | default | description / constraint |
|---|---|---|---|
| `path` | string | required | directory or wildcard, e.g. `file:///tmp/input/all-*.csv`, `hdfs://ns/data` |
| `default_fs` | string | `file:///` | used as the base only when `path` has no scheme; must carry a scheme |
| `file_format` | string | `text` | `text`/`csv`/`xlsx` |
| `schema_fields` | list | `[]` | required for `csv`/`xlsx`; not available for `text` |
| `header` | bool | `false` | `csv`/`xlsx` skip the first row; not available for `text` |
| `sheet` | string | `""` | `xlsx` worksheet name, empty = first; not available for other formats |
| `encoding` | string | `UTF-8` | `text`/`xlsx` fixed to UTF-8 |
| `csv_delimiter` | string | `,` | single character; only for `csv` |
| `csv_quote` | string | `"` | single character; only for `csv` |

### WriteToFilesystem

| parameter | type | default | description |
|---|---|---|---|
| `path` | string | required | output **directory** |
| `default_fs` | string | `file:///` | must carry a scheme |
| `file_format` | string | `text` | `text`/`csv`/`xlsx` |
| `schema_fields` | list | `[]` | required for `csv`/`xlsx` |
| `header` | bool | `false` | `csv`/`xlsx` writes a header |
| `sheet` | string | `""` | `xlsx` worksheet name |
| `encoding` | string | `UTF-8` | `text`/`xlsx` fixed to UTF-8 |
| `csv_delimiter` | string | `,` | single character |
| `csv_quote` | string | `"` | single character |
| `file_prefix` | string | `output` | file name prefix |
| `num_shards` | int | `0` | shard count, `0` = runner decides; **`xlsx` must be `1`** (one workbook is one zip container) |

---

## Elasticsearch 6

### ReadFromElasticsearch6

| parameter | type | default | description / constraint |
|---|---|---|---|
| `connection_uri` | string | required | comma-separated http/https nodes |
| `index` | string | `""` | single index; choose one of this and `indices` |
| `indices` | list(string) | `[]` | multiple indexes; mutually exclusive with `index` |
| `username` | string | `""` | |
| `password` | string | `""` | |
| `schema_fields` | list(`name:TYPE`) | `[]` | empty = single `document` column |
| `scan_query` | string | `""` | query DSL JSON, empty = full collection |
| `scroll_size` | int | `1000` | records per scroll page |
| `scroll_timeout_minutes` | long | `1` | scroll context lifetime |
| `scan_slices` | int | `1` | how many slices to hash-parallel-read by document ID, recommended equal to the shard count |

### WriteToElasticsearch6

| parameter | type | default | description |
|---|---|---|---|
| `connection_uri` | string | required | |
| `index` | string | required | |
| `username` | string | `""` | |
| `password` | string | `""` | |
| `schema_fields` | list(`name:TYPE`) | `[]` | empty = treat input `document`(STRING) as raw JSON to write |
| `batch_size` | int | `1000` | |
| `doc_type` | string | `_doc` | ES 6.x still requires a mapping type per document; leaving it unset would fail every write |

---

## Elasticsearch 8

Difference from 6: the read side uses **PIT** (point in time) instead of scroll, plus one more `keep_alive_minutes`.

### ReadFromElasticsearch8

| parameter | type | default | description / constraint |
|---|---|---|---|
| `connection_uri` | string | required | comma-separated http/https nodes |
| `index` | string | `""` | single index; choose one of this and `indices` |
| `indices` | list(string) | `[]` | multiple indexes; mutually exclusive with `index` |
| `api_key` | string | `""` | api key for auth (choose one of this and user/password) |
| `username` | string | `""` | |
| `password` | string | `""` | |
| `schema_fields` | list(`name:TYPE`) | `[]` | empty = single `document` column |
| `batch_size` | int | `1000` | records per page |
| `scan_query` | string | `""` | query DSL JSON, empty = match_all |
| `scan_slices` | int | `1` | parallel slice count, recommended equal to the shard count |
| `keep_alive_minutes` | int | `5` | PIT lifetime (must not exceed between two page turns of a single slice) |

### WriteToElasticsearch8

| parameter | type | default | description |
|---|---|---|---|
| `connection_uri` | string | required | |
| `index` | string | required | |
| `api_key` | string | `""` | |
| `username` | string | `""` | |
| `password` | string | `""` | |
| `schema_fields` | list(`name:TYPE`) | `[]` | empty = treat input `document`(STRING) as raw JSON to write |
| `batch_size` | int | `1000` | |

---

## RabbitMQ

Uses the official Java client. Connection fields (shared by read/write, written at the `config` top level):

| parameter | type | default | description |
|---|---|---|---|
| `host` | string | `localhost` | |
| `port` | int | `5672` | |
| `virtual_host` | string | `/` | |
| `username` | string | `guest` | |
| `password` | string | `guest` | |

### ReadFromRabbitMQ

Bounded snapshot: consumes up to `max_messages` from `queue` via `basicGet` (synchronous pull); finishes when the queue is empty.

| parameter | type | default | description / constraint |
|---|---|---|---|
| `queue` | string | required | |
| `max_messages` | int | `1000` | `0` = no limit, read until the queue is empty |
| `wait_timeout_ms` | long | `1000` | how long to wait for a message when the queue is empty before finishing; `0` = return immediately |

Fixed output schema: `exchange` STRING, `routing_key` STRING, `body` STRING (UTF-8), `message_id` STRING (nullable), `delivery_tag` LONG.

### WriteToRabbitMQ

| parameter | type | default | description / constraint |
|---|---|---|---|
| `queue` | string | required | |
| `exchange` | string | `""` | target exchange; empty = default (direct) exchange |
| `routing_key` | string | `""` | falls back to `queue` when blank |
| `body_field` | string | `body` | input row field holding the message body (STRING) |
| `routing_key_field` | string | `routing_key` | takes precedence over `routing_key` when the field is present |
| `exchange_field` | string | `exchange` | takes precedence over `exchange` when the field is present |
| `message_id_field` | string | `message_id` | absent/null means no message ID is set |
| `declare_exchange` | bool | `false` | declare the exchange before writing (no-op if it exists) |
| `exchange_type` | string | `direct` | `direct`/`fanout`/`topic`/`headers`; only checked when `declare_exchange: true` |
| `declare_queue` | bool | `false` | declare the queue before writing (no-op if it exists) |
| `persistent` | bool | `false` | delivery mode 2 (persistent) when true |
| `message_ttl_ms` | long? | `null` | no expiry when unset |
| `batch_size` | int | `100` | flush and wait for publisher confirms after this many messages |

---

## ClickHouse

JDBC-based. Connection fields (shared by read/write):

| parameter | type | default | description |
|---|---|---|---|
| `endpoint` | string | `http://localhost:8123` | includes protocol, e.g. `https://cloud.clickhouse.com` |
| `database` | string | `default` | |
| `username` | string | `default` | |
| `password` | string | `""` | |

### ReadFromClickHouse

| parameter | type | default | description / constraint |
|---|---|---|---|
| `query` | string | required | must be a SELECT; output schema is derived from the result-set metadata |
| `max_rows` | int | `0` | `0` = no limit |
| `connect_timeout_ms` | int | `10000` | |
| `socket_timeout_ms` | int | `60000` | |

### WriteToClickHouse

| parameter | type | default | description / constraint |
|---|---|---|---|
| `table` | string | required | column list for the INSERT follows the input row schema's field order |
| `batch_size` | int | `10000` | flush after this many rows accumulate |
| `max_retries` | int | `3` | on transient write failures (network errors, 5xx); `0` = no retries |
| `retry_delay_ms` | long | `1000` | exponential back-off base delay |
| `connect_timeout_ms` | int | `10000` | |
| `socket_timeout_ms` | int | `60000` | |
| `column_names` | list(string) | `[]` | explicit INSERT column names; empty = use the row schema's field names in order; when set, size must match the row's field count |

Write is at-least-once: a retried batch may duplicate rows if a prior attempt actually succeeded but the client lost the response. Use `ReplacingMergeTree` or dedupe downstream for exactly-once. Failed rows (after retries) go to the dead-letter stream.

---

## Cassandra

Uses the DataStax Java driver (CQL). Connection fields (shared by read/write):

| parameter | type | default | description |
|---|---|---|---|
| `endpoints` | list(string) | `["localhost:9042"]` | `host:port` contact points; at least one required |
| `keyspace` | string | required | |
| `datacenter` | string | `datacenter1` | default matches the single-node Testcontainers image |
| `connect_timeout_ms` | int | `10000` | |
| `request_timeout_ms` | int | `30000` | |

### ReadFromCassandra

| parameter | type | default | description / constraint |
|---|---|---|---|
| `query` | string | required | CQL SELECT; output schema derived from the result-set metadata |
| `consistency_level` | string | `LOCAL_ONE` | `LOCAL_ONE`/`LOCAL_QUORUM`/`ONE`/`QUORUM`/`ALL`/etc. |
| `max_rows` | int | `0` | `0` = no limit |
| `fetch_size` | int | `5000` | paging size |

### WriteToCassandra

| parameter | type | default | description / constraint |
|---|---|---|---|
| `table` | string | required | columns derived from the input row's schema |
| `consistency_level` | string | `LOCAL_ONE` | |
| `batch_size` | int | `50` | rows per CQL batch |
| `max_retries` | int | `3` | `0` = no retries |
| `retry_delay_ms` | long | `1000` | multiplied by attempt number |
| `unlogged_batch` | bool | `false` | faster, but not atomic across partitions; the default LOGGED batch only guarantees atomicity within one partition |

Batch size should stay modest — Cassandra's per-batch size limit is 5 KB on older versions, 1 MB on 4.x+. Failed rows (after retries) go to the dead-letter stream.

---

## Amazon SQS

Uses the AWS SDK v2 SQS client. Connection fields (shared by read/write):

| parameter | type | default | description |
|---|---|---|---|
| `queue_url` | string | required | full queue URL including account ID and queue name |
| `region` | string | `us-east-1` | ignored when `endpoint_override` is set |
| `endpoint_override` | string | `""` | custom endpoint for local emulators (e.g. LocalStack `http://localhost:4566`) |
| `access_key_id` | string | `""` | |
| `secret_access_key` | string | `""` | |

### ReadFromSQS

Bounded snapshot: long-polls until `max_messages` messages have been emitted, receiving up to `batch_size` per call; messages are deleted after read when `delete_after_read` is true. Set `streaming: true` with `max_messages: 0` to keep polling as an unbounded source.

| parameter | type | default | description / constraint |
|---|---|---|---|
| `max_messages` | int | `10` | emitted message limit; `0` = no limit |
| `batch_size` | int | `10` | messages per ReceiveMessage call, `1`-`10` |
| `visibility_timeout` | int | `30` | seconds a received message is hidden from other consumers, `0`-`43200` |
| `wait_time_seconds` | int | `20` | long-poll wait when the queue is empty, `0`-`20`; `0` = short poll |
| `delete_after_read` | bool | `true` | |
| `streaming` | bool | `false` | continue after an empty receive; requires `max_messages: 0` |

Fixed output schema: `message_id` STRING, `body` STRING, `receipt_handle` STRING, `attributes` MAP<STRING, STRING>.

### WriteToSQS

| parameter | type | default | description / constraint |
|---|---|---|---|
| `body_field` | string | `body` | input row field holding the message body |
| `message_group_id_field` | string | `""` | FIFO message group ID; absent means a non-FIFO queue |
| `message_deduplication_id_field` | string | `""` | FIFO deduplication ID; absent means server-generated |
| `batch_size` | int | `10` | messages per SendMessageBatch call, `1`-`10` |

---

## Prometheus

Read-only, plain HTTP client against the Prometheus HTTP API — no driver dependency.

### ReadFromPrometheus

Executes a PromQL **instant query** (`/api/v1/query`); one row per returned time series.

| parameter | type | default | description / constraint |
|---|---|---|---|
| `endpoint` | string | `http://localhost:9090` | Prometheus server base URL |
| `query` | string | required | PromQL, e.g. `up`, `rate(http_requests_total[5m])` |
| `time` | string | `""` | evaluation timestamp; empty = server default (now) |
| `connect_timeout_ms` | int | `10000` | |
| `read_timeout_ms` | int | `30000` | |

Fixed output schema: `metric_name` STRING, `labels` MAP<STRING, STRING>, `value` DOUBLE, `timestamp` DOUBLE (unix epoch seconds). Only the `vector` result type is supported; other PromQL result types (`matrix`/`scalar`/`string`) produce no rows and a warning log. There is no write side — Prometheus is a pull-based metrics system.

---

## DynamoDB

Uses the AWS SDK v2 DynamoDB client. Connection fields (shared by read/write):

| parameter | type | default | description |
|---|---|---|---|
| `table_name` | string | required | |
| `region` | string | `us-east-1` | ignored when `endpoint_override` is set |
| `endpoint_override` | string | `""` | custom endpoint for local emulators (e.g. DynamoDB Local `http://localhost:8000`) |
| `access_key_id` | string | `""` | |
| `secret_access_key` | string | `""` | |

### ReadFromDynamoDB

Bounded snapshot: a single Scan, or a Query when `key_condition_expression` is set. Output schema is derived from the item attributes at runtime.

| parameter | type | default | description / constraint |
|---|---|---|---|
| `consistent_read` | bool | `false` | `false` = eventually consistent |
| `filter_expression` | string | `""` | server-side filter, e.g. `attribute_exists(pk)` |
| `projection_expression` | string | `""` | limit returned attributes, e.g. `pk, sk, data` |
| `key_condition_expression` | string | `""` | when set, a Query is used instead of a Scan |
| `expression_attribute_values` | map(string→string) | `{}` | values for the filter / key condition expressions |
| `max_items` | long | `0` | `0` = no limit |

### WriteToDynamoDB

Each row is converted to an item and written via BatchWriteItem (up to 25 items/call); failed items are retried individually, then sent to the dead-letter stream. Row field names become DynamoDB attribute names; the row schema's field types drive the attribute value types (STRING→S, numeric→N, BYTES→B, BOOLEAN→BOOL, etc.).

| parameter | type | default | description / constraint |
|---|---|---|---|
| `batch_size` | int | `25` | items per BatchWriteItem call, `1`-`25` |
| `max_retries` | int | `3` | `0` = no retries |
| `retry_delay_ms` | long | `500` | multiplied by attempt number |

---

## Dead letter (error_handling)

Written inside the **sink's `config`**, stripped by the framework and then handed to the config class for binding. Syntax aligned with Beam YAML:

```yaml
config:
  error_handling:
    output: errors        # the dead-letter stream is exposed to the DAG as <node name>.errors, and must be consumed downstream
```

- `output` (required): the dead-letter stream label name, referenced externally as `WriteOrders.errors` (see `examples/dead-letter.yaml`).
- Once declared, it **must** have downstream consumption, otherwise graph construction errors directly (to avoid "thought it was caught but actually lost").
- `threshold` (error-rate threshold): **not yet implemented**; declaring it errors at graph construction rather than silently failing.

The dead-letter record schema is `{element: ROW<original record>, error_type, error_message, transform}`,
and `StripErrorMetadata` can strip the metadata to restore the original record. The dead-letter record carries **the original row's own timestamp and window**,
and can be directly fed to `LogForTesting` or written back to a backup table.

---

## Connector development and service tests

New connectors must document their bounded or streaming read model, retry and acknowledgement
behavior, schema conversion rules, dead-letter behavior, serialization boundary, and SDF progress
unit. Add a real-service integration test under the `integration-tests` profile when a lightweight,
version-compatible container or emulator exists; logic-only tests do not replace protocol-level
coverage.

The normal `mvn test` suite remains self-contained. FTP uses the in-process Apache FtpServer,
which exercises the real FTP commands required by this connector (`REST`, `STOR`, `APPE`, and
`RNFR`/`RNTO`); a rootless-Podman container would mainly introduce passive-mode networking noise.
HBase has no maintained official image matching the 2.6.1 client, so its container test is deferred
until a version-compatible service is available.
