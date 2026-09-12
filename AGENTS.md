# AGENTS.md

HData —— an Apache Beam-based data synchronization/ETL tool, written in Kotlin, with job configuration in YAML, aligned with the Beam YAML specification.

## Build environment
- Built with **JDK 17** (target bytecode Java 17, Kotlin `jvmTarget` set to 17). Kotlin 2.4.20 is used throughout.
- Uses Maven 3.9+ (`mvn` command, **no Maven wrapper**); dependencies go through the aliyun mirror in `~/.m2/settings.xml`, so the first build needs network access.
- Jackson uses **3.x** (package name `tools.jackson.*`), but annotations are still `com.fasterxml.jackson.annotation`. Note the Jackson 3 API renames: `isTextual`→`isString`, `asText`→`asString`, `fieldNames`→`propertyNames`.

## Module structure
- `hdata-core`: the core engine and program entry point. `me.jayer.hdata.core.HData`'s `main` is the only entry point.
  - `spec/`: the syntax tree for pipeline files, YAML parsing, variable substitution, and structural validation. **YAML only** (TOML/JSON have been removed).
  - `spi/`: connector extension points. `HDataTransform` extends Beam's `SchemaTransform`, i.e. `PCollectionRowTuple -> PCollectionRowTuple`.
  - `registry/`: the `type` -> provider registry, which also bridges to Beam's native `SchemaTransformProvider` on the classpath.
  - `graph/`: syntax tree -> Beam DAG, handling chain/composite, reference resolution, topological sorting, dead letter, and windows.
  - `transforms/`: built-in transforms (Create / MapToFields / Flatten / LogForTesting / StripErrorMetadata / AssertEqual).
- `hdata-jdbc`: JDBC connector, `ReadFromJdbc` / `WriteToJdbc`.
  - `internal/`: implementation details. `TypeMappings` is an **immutable** rule table, parsed once per column,
    and the parse result is serialized with the DoFn and sent downstream — when adding things here, be careful **not to capture ordinary Kotlin lambdas**,
    since capturing `Function1` makes the entire DoFn non-serializable (use a serializable fun interface such as `ValueConverter`).
  - `partition/`: selection and validation of the partition column. `transform/`: three DoFns; connection pools are always created in `@Setup` and closed in `@Teardown`.
- `hdata-kafka`: `ReadFromKafka` / `WriteToKafka`. Reads **directly reuse Beam's `ReadFromKafkaDoFn`**,
  and this module only handles `internal/KafkaOffsets`: translating Flink-style startup/bounded modes into per-partition start/end offsets.
- `hdata-hive`: `ReadFromHive` / `WriteToHive`. **Does not go through JDBC / HiveServer2**;
  instead it mirrors Trino's Hive connector layer by layer: it pulls metadata from the metastore and reads/writes the data files directly under the table directory.
  - `metastore/`: the `HiveMetastore` interface + an immutable metadata model. The production implementation `ThriftHiveMetastore` speaks the
    metastore's thrift protocol directly (using the IDL-generated `ThriftHiveMetastore.Client`, not `HiveMetaStoreClient`:
    the latter drags in a whole set of server-side dependencies — derby / grpc / curator / zookeeper — whereas we only use 6 read-only calls).
    The test implementation `InMemoryHiveMetastore` is globally cached by name, enabled via `memory://<name>`, and is **only valid within a single JVM**.
    `PartitionNames` is the encode/decode for partition names, with rules aligned character-by-character with Hive's `FileUtils.escapePathName` / `makePartName`.
  - `type/`: recursive-descent parsing of Hive type strings (nested types cannot be split on commas) and value conversion.
    `timestamp` maps to `DATETIME` (wall-clock time), while `timestamp with local time zone` maps to `TIMESTAMP`.
  - `split/`: column directories, split feasibility determination, and path normalization.
  - `format/`: readers/writers for eight storage formats; readers uniformly implement `HiveRecordReader` (see the SDF section below).
    The RCFile container format and LazyBinary single-value encoding under `rcfile/` are **implemented from scratch against the format spec** —
    the `RCFile` class lives inside an 80MB hive-exec jar, and that jar bundles unrelocated copies of avro / orc / parquet / protobuf.
    Bidirectional compatibility with Hive has been verified (see the tests section).
  - `transform/`: DoFns for column files, the byte-range SDF read, row-to-record conversion, and partition registration.
- `hdata-mongodb`: `ReadFromMongoDb` / `WriteToMongoDb`. `internal/MongoBuckets` uses `$bucketAuto` to compute `_id`
  bucket boundaries, and reads are split by bucket index; writes go through `bulkWrite` and support `upsert_keys`.
- `hdata-hbase`: `ReadFromHBase` / `WriteToHBase`. Scans **reuse Beam's `HBaseIO.readAll()`**
  (note: not `HBaseIO.read()`, whose internals still use the old `BoundedSource`). `HBaseRowCodec` handles row encode/decode.
- `hdata-ftp`: `ReadFromFtp` / `WriteToFtp`. Reads in parallel **by byte range**, using FTP's `REST` command to locate the start;
  writes are sharded, with each bundle first writing `.tmp` then `rename`.
- `hdata-filesystem`: `ReadFromFilesystem` / `WriteToFilesystem`. Matching uses `FileIO.match()`,
  text reads use `TextIO.readFiles()` (true byte-range splitting), and writes use `FileIO.write()` (sharding + atomic rename).
- `hdata-elasticsearch-6` / `hdata-elasticsearch-8`: parallel reads by ES-native **slice** (`scan_slices`).
- `hdata-redis`: `ReadFromRedis` / `WriteToRedis`, using the Redisson client. Reads support `scan` / `keys` / `stream`
  modes; key collection (SCAN / XRANGE) is done once on the driver side (bounded snapshot) before fetching values one by one;
  `stream` mode's `start_id` / `end_id` parse as `-` / `+` / `<millis>-<seq>` and genuinely bound the XRANGE range.
- `hdata-neo4j`: `ReadFromNeo4j` / `WriteToNeo4j`, using the official Java Driver. The read side **can be graphed without connecting to the database**
  (the output schema is declared via `schema_fields`); the write side executes Cypher, binding row fields to `$param` by name,
  committing one transaction per `batch_size` rows, and falling back to per-row writes on batch failure to locate bad data (same as `WriteToJdbc`).
- `hdata-iceberg`: `ReadFromIceberg` / `WriteToIceberg`, using HadoopCatalog. The write side auto-creates the table if it does not exist,
  writing one AVRO data file per bundle before committing; see the Iceberg section below for `write_mode`.
- `hdata-debezium`: `ReadFromDebezium`, a CDC source based on the Debezium embedded engine. The output schema is fixed
  (`op` / `key` / `before` / `after` / `source` / `ts_ms`), so a single pipeline can capture multiple tables of differing structures at once.
- `hdata-rabbitmq`: `ReadFromRabbitMQ` / `WriteToRabbitMQ`, using the `com.rabbitmq:amqp-client`. The read side performs a bounded
  snapshot using synchronous `basicGet` (pulls up to `max_messages` from the queue); the write side publishes with publisher confirms
  and supports batching, persistent delivery, message TTL, and dead-letter output.
- `hdata-clickhouse`: `ReadFromClickHouse` / `WriteToClickHouse`, using the ClickHouse JDBC driver. The read side executes a SQL query
  and derives the output schema from the result set metadata at runtime; the write side batches rows into INSERT statements with
  configurable retries and dead-letter support. Type mapping covers ClickHouse's full type hierarchy including `Nullable`,
  `LowCardinality`, and `Decimal` variants.
- `hdata-cassandra`: `ReadFromCassandra` / `WriteToCassandra`, using the DataStax Java driver 4.x. The read side executes a CQL SELECT
  query and derives the output schema from the result metadata; optional `parallel_scan_segments` with `partition_key_column`
  enumerates Cassandra token ranges and scans them independently; the write side batches rows into CQL batch INSERT statements with
  configurable consistency levels (ONE / QUORUM / ALL / LOCAL_*) and retries. Type mapping covers Cassandra's scalar, collection
  (`List`, `Set`, `Map`), and `Tuple` types.
- `hdata-sqs`: `ReadFromSQS` / `WriteToSQS`, using the AWS SDK v2 for SQS. The read side performs a bounded snapshot via
  long-polling (`ReceiveMessage`), with configurable wait time, visibility timeout, and delete-after-read; the write side
  batches messages via `SendMessageBatch` (up to 10 per call), with FIFO queue support (`message_group_id` / `message_deduplication_id`).
  LocalStack or `elasticmq` can be used for integration testing.
- `hdata-prometheus`: `ReadFromPrometheus` (read-only). Executes a PromQL instant query (`/api/v1/query`) over HTTP and returns
  one row per time series. The output schema is fixed: `metric_name` (STRING), `labels` (MAP<STRING, STRING>), `value` (DOUBLE),
  `timestamp` (DOUBLE). A local Prometheus instance with scraped targets can be used for testing.
- `hdata-dynamodb`: `ReadFromDynamoDB` / `WriteToDynamoDB`, using the AWS SDK v2 for DynamoDB. The read side executes a Scan
  (or Query when `key_condition_expression` is provided) and derives the output schema from item attributes at runtime; the write
  side uses `BatchWriteItem` (up to 25 items per call) with retries and dead-letter support. DynamoDB is schemaless, so all
  fields are nullable. `amazon/dynamodb-local` is used for integration testing.

Config classes depend only on `TransformConfig.bind(...)` (Jackson 3); do not instantiate your own `YAMLMapper`;
on the write path, manage resources with `@Setup`/`@FinishBundle`/`@Teardown`, and failed rows go to the dead letter via `ErrorSchemas.failure(...)`.

## Read-side Splittable DoFn policy (important)

Every connector source must be implemented as a Splittable DoFn (SDF), including a source whose
only safe split is a single logical subscription. This is a correctness requirement, not merely a
parallelism preference: an SDF supplies a checkpoint boundary, cooperative resumption, progress,
and (for unbounded reads) a watermark contract needed by streaming runners and downstream
windowing. Do not add a new source as a plain `ParDo` over a synthetic trigger element.

For a bounded source, make the restriction represent a real range whenever one exists (file bytes,
token ranges, key ranges, pages, offsets, or native slices). For a non-partitionable queue or CDC
subscription, use an unbounded SDF with a synthetic monotonically increasing emitted-record
restriction. It must return `ProcessContinuation.resume()` after a finite work budget and advance a
watermark from the source event timestamp when available. RabbitMQ and Debezium are the reference
implementations for this latter pattern.

All current in-repository connector read DoFns follow this policy. A migration must preserve the
connector's batch semantics and add a focused test for restriction completion/resumption; do not
relabel a plain DoFn as an SDF without defining a durable unit of progress.

## Read-side progress model

Every custom connector source is now an SDF. Choose the smallest real unit of progress rather than inventing byte ranges that a backend cannot safely resume.

**Reuse Beam's official IOs instead of writing your own**: Kafka / HBase / Filesystem have already been switched to the official implementations (see the module notes above).
For the remaining modules (JDBC / Hive / MongoDB / Elasticsearch / FTP), Beam has no SDF implementation, so they are written by hand.

JDBC, Hive, MongoDB, Elasticsearch, FTP, Cassandra, and DynamoDB use backend-native ranges, slices, or segments. Redis uses keys or stream entries; Iceberg uses manifest data-file splits; ClickHouse, Neo4j, Prometheus, and Pulsar each use one bounded snapshot restriction when no safe finer split exists.

Iceberg and DynamoDB enumerate independent chunks before their SDF stage: one `IcebergFileSplit` per data file and one native scan segment per DynamoDB segment. The runner can schedule those elements independently while the SDF supplies a completion and retry boundary for each chunk.

RabbitMQ, SQS, and Debezium are unbounded SDFs. Their synthetic emitted-record restriction provides checkpointing and cooperative resumption, while RabbitMQ/SQS use broker routing for parallelism. Debezium persists its true source offset itself and advances its watermark from `ts_ms`. A future persistent Pulsar consumer must first introduce a named subscription and acknowledgement policy before it becomes unbounded.

When writing your own SDF, there are four iron rules, all learned from this round of refactoring:

1. **Do not `tryClaim(range.to - 1)` to claim the whole range at once**. That tells Beam "this range is not further splittable",
   so at runtime it can neither distribute the remaining work to idle workers nor report progress. The correct approach is to claim one unit at a time in a loop.
   The claim granularity follows each format's smallest locatable unit: for Hive, ORC uses stripes, Parquet uses row groups,
   Avro / SequenceFile / RCFile use sync blocks, and text uses lines.
2. **`checkDone()` has a contract**: `OffsetRangeTracker` requires the offset of the last *attempt* to be >= `to - 1`.
   When you finish reading early (e.g. the file hits EOF), you must add one more `tryClaim(range.to)` — it returns false but records the attempt,
   otherwise it reports `claiming work in [x, y) was not attempted`.
   Conversely, **after a claim is rejected you must not add another**, otherwise it throws directly because "the claimed offset is less than the last attempt",
   so `HiveRecordReader.read()` distinguishes "finished" from "rejected" via its return value.
   Likewise, claimed offsets must be **strictly increasing**: in block-compressed SequenceFiles, the `getPosition()` of several consecutive rows is the same value,
   so you must wait until the position actually advances before claiming.
3. **Some formats are inherently not splittable by byte**: a quoted CSV field can embed newlines, so cutting at an arbitrary byte position would split a record in half;
   xlsx is a zip container that can only be parsed from the start; a whole-file gzip / snappy-compressed text can only be decompressed from the start;
   tables configured with `skip.header.line.count > 1` or `skip.footer.line.count` must also be read as a whole file.
   For these, parallelism comes from the number of files, and the code must state why clearly.
4. **Line-based splitting must start reading from `from - 1`, not `from`**. If exactly one line starts at `from`,
   then `from - 1` is the previous line's newline; doing one `readLine()` first only consumes that newline, and the line still belongs to this split;
   reading directly from `from` and dropping the first line would make this entire line **disappear** — the previous split stopped when its position reached `from`,
   so it will not read it either. Beam's own `TextSource` does exactly this.

## Hive connector boundaries and pitfalls

Supported storage formats (read/write both directions, determined by the SerDe + InputFormat on the metastore; if it cannot be determined it **throws directly**, no guessing):
`TEXTFILE` / `CSV`(OpenCSVSerde) / `SEQUENCEFILE` / `RCTEXT` / `RCBINARY` / `ORC` / `PARQUET` / `AVRO`.

Explicitly **unsupported** and will error at graph-construction time:
- **Transactional (ACID) tables**: the directory contains `delta_*` / `base_*` with row-level insert/delete/update markers, so reading directly from files would read rows that have already been deleted;
- **Views**; `uniontype`;
- **Nested types** under `RCBINARY` (array / map / struct) — there is no real sample to verify the nested encoding of LazyBinary,
  so rather than ship an unconfident implementation, let the user switch to ORC/Parquet. `RCTEXT` uses text encoding and fully supports nested types.

Two cases of **format-intrinsic** information loss — not implementation problems, but worth knowing:
- A 0-length cell in `RCBINARY` could be either NULL or an empty string; it is always read as NULL (Hive does the same);
- Empty fields in `CSV` are likewise indistinguishable, so the write side writes null as `\N` rather than an empty string.

The write side requires the **target table to already exist** (table creation is DDL and should not be done by the sync job); the table's format dictates how it is written,
partition values come from the row's partition columns, encoded into the directory name, with **the partition columns excluded from the data files**; after writing, the new partitions are registered with the metastore
(otherwise the files sit in the directory but Hive cannot see them).

Partitions are always **dynamic**: each row decides which partition it lands in based on its own partition-column values, and a single job can write an arbitrary number of partitions,
consistent with Hive's dynamic partition insertion. When the upstream has no partition columns, use `MapToFields` to add a constant column — there is no separate static-partition config.
`write_mode` decides what happens to existing data:
- `append` (default) = `INSERT INTO`, new files are added and old files are left untouched;
- `overwrite` = `INSERT OVERWRITE`, which **only clears the old files in the partitions actually written this run**;
  partitions with no data landing in them are untouched, consistent with Hive's dynamic-partition-overwrite semantics. To empty the whole table, do the DROP yourself.

There is a must-know pitfall with append writes: Beam's `FileIO.Write.defaultNaming` names files only by
`prefix-shardNumber-of-total`, with **no job identifier**, so running the same table twice produces identical file names,
and the second run silently overwrites the first run's result — neither an append nor an overwrite, yet the job status is still SUCCESS.
Therefore the file name must include a per-job unique marker (`HiveSink.runToken()`),
which `HiveWriteModeTest` specifically pins down.

Overwrite is not atomic: in the short window where the new files have been renamed into place but the old files are not yet fully deleted, a reader will see both sets of data at once.
A non-ACID Hive table is like this anyway (Hive itself is too); for strong consistency you need a transactional table — which is not supported here.

There are three dependency pitfalls that only blow up at runtime; read this before touching the pom:
1. `orc-core` must use the `shaded-protobuf` classifier, **and you must explicitly declare the likewise-shaded `orc-format`** —
   orc-core's pom declares orc-format without a classifier, and mixing them causes
   `NoSuchMethodError: OrcProto$StripeFooter.writeTo(org.apache.orc.protobuf.CodedOutputStream)` when writing a stripe.
2. `parquet-hadoop`'s code path reaches `org.apache.hadoop.mapreduce.lib.input.FileInputFormat`,
   and without `hadoop-mapreduce-client-core` there is no compile-time warning, only a runtime `NoClassDefFoundError`.
3. The write side hands file writing to Beam's `FileIO`, so you must include `beam-sdks-java-io-hadoop-file-system`,
   otherwise writing tables on HDFS reports "No filesystem found for scheme hdfs".

Also, the location given by the metastore is a scheme-bearing URI that must pass through
`HivePaths.forBeamIO` before being handed to Beam's `FileIO.write()`: Beam's `LocalFileSystem` behaves inconsistently for `file://` on read vs. write,
and on write it treats the whole string as a relative path, dropping the data into a directory named `file:` under the current working directory, **yet the job status is still DONE**.
`hdata-filesystem`'s `FilesystemPaths` has the same pitfall.

## Iceberg's write_mode

`append` (default) means each bundle writes one data file and then `newAppend().commit()`.

`overwrite` means "first clear the table, then write this run's data", and the table clear **must happen exactly once, and must be ordered before all writes**.
This cannot go into the write side's `@Setup`: doing so, the second bundle would delete the data the first bundle just wrote,
leaving "overwrite" with only the last bundle's contents while the job status is still SUCCESS.
So the table clear is a separate `IcebergTruncateFn`, attached to the write ParDo via a **side input** —
a ParDo with a side input will not process any main input until the side-input PCollection is fully computed, which guarantees the ordering. Think this through before changing this part.

The table clear itself uses `newDelete().deleteFromRowFilter(alwaysTrue())` as a single atomic commit, rather than deleting files yourself:
a reader sees either the old snapshot or the empty table, never a half-deleted intermediate state; deleting again on an empty table is a no-op,
so bundle retries are safe.

Two known boundaries: the written data file is fixed as AVRO and does not follow the table's own write format;
only basic scalar types are supported (see `internal/IcebergSchemas`), and nested / list / map are not yet done.

## Running
- `me.jayer.hdata.core.HData --pipeline=<file>`, plus `--dryRun` (graph construction only, then print) and `--waitUntilFinish`.
- The pipeline file's top level is `pipeline:` plus optional `options:`; `pipeline` itself is a composite/chain-form transform node.
- Use top-level `execution.mode: auto | batch | streaming` to declare runner semantics. `auto` selects streaming when any configured source reports `SourceMode.UNBOUNDED`; `batch` rejects such a graph. Every new source must override `TransformProvider.sourceMode(config)` when its boundedness changes with configuration.
- See `examples/` for samples; see `docs/ARCHITECTURE.md` for design notes.
- **The runner is a Maven profile**: by default only DirectRunner, plus `-Pflink-runner` (beam-runners-flink-2.2)
  and `-Pspark-runner` (beam-runners-spark-4), both verified to run end-to-end jobs on JDK 17.
  Historically the runner dependencies excluded the engine's own jars, which would make `PipelineOptionsFactory`'s static initialization fail outright — do not write it that way again.
- Spark relies on three coordinated pieces; changing any one of them breaks it:
  1. The root pom's `hadoop.version=3.5.0` raises the Hadoop that Spark transitively pulls in. Hadoop 3.4.1's `UserGroupInformation`
     uses the legacy `Subject.getSubject(...)` API; Hadoop 3.5.0 uses `Subject.current()` instead.
  2. `-Pspark-runner` excludes `spark-connect-shims_2.13` (pulled in transitively via spark-sql-api): its stub
     `SparkConf` shadows the real class from spark-core, causing `NoSuchMethodError: SparkConf.contains`.
  3. Spark itself is `provided` (supplied by spark-submit in production); to run locally via `java -cp` you must also add `-Pspark-local`.
- Neither runner needs `--add-opens`.

## Extending a new connector (conventions)
- Implement `me.jayer.hdata.core.spi.TransformProvider`, where one provider handles only one `type` (one each for read and write).
  Using `TypedTransformProvider<C>` lets you bind config directly to a Kotlin data class.
- Return one of the three convenience base classes `RowSource` / `RowTransform` / `RowSink`; `RowSink.write` returns the dead-letter stream (returns null if unsupported).
- You must register at `src/main/resources/META-INF/services/me.jayer.hdata.core.spi.TransformProvider`.
- Config keys use `snake_case` (the mapper has the SNAKE_CASE naming strategy installed), and **`FAIL_ON_UNKNOWN_PROPERTIES` is on** —
  so when adding a field you must also update the config class, otherwise the user gets an error when they write it.
- Connectors should **not** touch `YAMLMapper` directly; use only `TransformConfig`; `error_handling` is stripped by the framework
  and passed in via `TransformConfig.errorHandling`.
- **A config option must either actually take effect, or not be accepted at all**. "Declared, `validate` let it through, but the implementation never reads it" is a recurring
  class of bug in this repo, and none of them fail the job — they only make the user think their config did something:
  Kafka's `key_format` / `value_format`, Redis's `start_id` / `end_id`, Neo4j's `batch_size`,
  and Iceberg's `write_mode` / `catalog_name` have all gone bad this way. When adding a config option, also write a test that asserts it
  **genuinely changes behavior** — asserting only that `validate` does not throw will not catch it. For capabilities not yet doable, **error explicitly**
  like `KafkaWriteConfig` rejects `exactly-once`, rather than silently degrading.
- Dead-letter records always carry **the original row's own timestamp and window** (`ValueInSingleWindow`).
  Making up `Instant.now()` + `GlobalWindow` gives neither replayability nor, in a windowed pipeline, a working `context.output` (it would throw); the error must use the real exception, not be fabricated from row fields.

## Testing
`mvn test` runs everything (~510 cases), **without requiring any external service**. The stub strategies for end-to-end tests:

| Module | End-to-end approach |
|---|---|
| JDBC | H2 in-memory database |
| Hive | local temp directory as warehouse + `InMemoryHiveMetastore` (`memory://`), all eight formats **really write files and really read them back** |
| Kafka | `KafkaIO.withConsumerFactoryFn` injects Kafka's own `MockConsumer`, **really runs Beam's SDF**; write side uses `MockProducer` |
| FTP | Apache FtpServer runs in-process, covering real `REST` / `STOR` / `APPE` / `RNFR-RNTO` |
| Filesystem | local temp directory |
| Redis | embedded-redis runs a real in-process Redis, running the full pipeline |
| Iceberg | local temp directory as warehouse + HadoopCatalog, **really writes files and really reads them back** |
| Debezium | Debezium's own database-free `SimpleSourceConnector`, **really starts the embedded engine** |
| Neo4j | no lightweight in-process stub, so Mockito fakes `Driver` / `Session` / `Transaction`, covering row mapping, parameter binding, batching, and per-row fallback |
| HBase / MongoDB / Elasticsearch | no lightweight in-process stub, so only the pure-logic layer of codecs, splitting, and config validation is covered |
| RabbitMQ | config binding, serialization, and dead-letter tests in unit; real-service container test under `-Pintegration-tests` |
| ClickHouse | config binding, type-mapping, serialization, and dead-letter tests in unit; real-service container test under `-Pintegration-tests` |
| Cassandra | config binding, serialization, and dead-letter tests in unit; real-service container test under `-Pintegration-tests` |
| Amazon SQS | config binding, serialization tests in unit; LocalStack container test under `-Pintegration-tests` |
| Prometheus | config binding, serialization, and mock-server parsing tests in unit |
| DynamoDB | config binding, serialization, and dead-letter tests in unit; `amazon/dynamodb-local` container test under `-Pintegration-tests` |

When writing connector tests, include at least one `SerializableUtils.ensureSerializable(...)`:
a DoFn that captures a non-serializable object only blows up **when the job is submitted**, never through a unit test that only calls `processElement`.
This is how ES 8's `Query` / `SortOptions` slipped in.

When calling `@ProcessElement` / `@SplitRestriction` directly in a unit test, use
`me.jayer.hdata.core.testing.CollectingOutputReceiver` — Beam's `OutputReceiver` is not a SAM interface,
and writing your own stub would take hundreds of lines.

`hdata-core`:
- `spec/PipelineSpecLoaderTest`, `spec/WindowingSpecTest`: parsing, format validation, variable substitution, window declaration.
- `graph/PipelineGraphBuilderTest`: chain/composite/dead-letter/nested end-to-end behavior.
- `graph/PipelineWiringTest`: port validation, composite-node output, window propagation.
- `transforms/BuiltinTransformsTest`: normal and error paths for each built-in transform.
- `util/RowConvertersTest`, `error/ErrorSchemasTest`, `spi/TransformConfigTest`, `registry/TransformRegistryTest`.
- `spec/ExamplesTest`: all examples under `examples/` must parse; run this when changing examples.
- Test connectors live in `src/test/kotlin/.../testing/TestSinkProvider.kt`, registered via the test resources' services file.

`hdata-jdbc`:
- `JdbcPipelineTest` runs real SQL on an **H2 in-memory database**, covering table read / query read / partitioned parallel read / sharded-range read /
  batch write / dead letter. The fixture is `H2Database`; note its URL carries `DB_CLOSE_DELAY=-1`:
  JdbcSource opens a connection once at graph-construction time to infer the schema and then closes it, and without this parameter the in-memory DB vanishes on the spot.
- Pure-logic tests: `internal/SqlTest`, `internal/JdbcMetadataTest`, `internal/TypeMappingsTest`, `partition/*Test`, `JdbcConfigTest`.
- When asserting behavior, prefer putting the expectation into the pipeline file's `AssertEqual`, then verifying written DB results with `H2Database.queryColumn`.
- **H2 uppercases unquoted identifiers**, so use `ID`/`NAME` for column names, not `id`/`name`.
- To verify "does partitioned read actually take effect", look at the PCollection's full name (`assertReadStrategy`): once primary-key detection fails
  it **silently** degrades to single-partition read, and only asserting row count will not catch it.
- Different DBs report inconsistent `columnClassName` for the same SQL type (H2's SMALLINT reports `Integer`, CLOB reports
  `java.sql.Clob`, MySQL's CLOB reports `String`), so do not hardcode Java types in assertions.

`hdata-hive`:
- `HivePipelineTest` runs end-to-end with **local temp directory + in-process metastore** (fixture `TestHive`):
  all eight storage formats written and read back, partition directories, partition registration, partition pruning, column projection, byte-range splitting.
- `HiveTypeRoundTripTest` verifies round-trips type by type, including **all-null rows** —
  nulls are most likely to silently become `false` / `""` in some format (seen with CSV empty fields and RCBINARY 0-length cells).
- `RcFileTest` has an RCFile fixture **truly written by hive-exec 4.0.1**
  (`src/test/resources/hive-written.rc`), used to pin down that our hand-rolled container format really is compatible with Hive.
  The reverse is also verified: Hive's `RCFile.Reader` can fully read the file written by this implementation, and the two outputs
  are byte-for-byte identical except for that 16-byte random sync marker. The fixture is pre-generated and checked into the repo, so the test does not depend on hive-exec.
- `HiveWriteModeTest` specifically tests problems that **only surface on the second write**: append must not overwrite the previous run, overwrite must only clear the partitions written this run,
  dynamic-partition registration of a new partition must not error on old partitions, and two jobs' file names must not collide.
- `HiveSerializationTest` covers serialization of all DoFns, `FileIO.Sink`, and the metadata model.
- Pure-logic tests: `type/HiveTypesTest` (nested-type parsing), `metastore/PartitionNamesTest` (escape rules),
  `format/HiveFormatsTest` (format determination / splittability / text codec), `HiveConfigTest` (config binding and dead letter).

All remaining modules have tests; see the stub table above for how deep coverage goes: `hdata-ftp` / `hdata-filesystem` / `hdata-redis` /
`hdata-iceberg` / `hdata-debezium` have real end-to-end; `hdata-mongodb` / `hdata-hbase` /
`hdata-elasticsearch-6` / `hdata-elasticsearch-8` only reach the pure-logic layer. When adding end-to-end for them
refer to `hdata-kafka`'s Splittable DoFn and `hdata-jdbc`'s H2 approach.

`hdata-rabbitmq`:
- `RabbitMQReadConfigTest` / `RabbitMQWriteConfigTest`: config binding and validation (blank host, invalid port, blank queue, etc.).
- `RabbitMQSerializationTest`: ensures `RabbitMQReadFn` and `RabbitMQWriteFn` are serializable.
- `RabbitMQContainerIT` (under `-Pintegration-tests`): real RabbitMQ container; writes messages and reads them back;
  verifies dead-letter capture when a row is missing the body field.

`hdata-clickhouse`:
- `ClickHouseReadConfigTest` / `ClickHouseWriteConfigTest`: config binding and validation (blank endpoint, blank table, zero batch_size, etc.);
  also tests `resolvedColumns` for explicit `column_names` and mismatched sizes.
- `ClickHouseTypeMappingsTest`: verifies ClickHouse-to-Beam type mapping for basic types, `Nullable`, `LowCardinality`,
  `Decimal` parameters, unsigned integers, and unknown types.
- `ClickHouseSerializationTest`: ensures `ClickHouseReadFn` and `ClickHouseWriteFn` are serializable.
- `ClickHouseContainerIT` (under `-Pintegration-tests`): real ClickHouse container; creates a MergeTree table,
  writes rows via the write provider, reads them back via the read provider, and verifies dead-letter on type mismatch.

`hdata-cassandra`:
- `CassandraReadConfigTest` / `CassandraWriteConfigTest`: config binding and validation (empty endpoints, invalid port, blank keyspace, etc.).
- `CassandraSerializationTest`: ensures `CassandraReadFn` and `CassandraWriteFn` are serializable.
- `CassandraContainerIT` (under `-Pintegration-tests`): real Cassandra container (cassandra:4.1);
  creates keyspace and table, writes rows, reads them back, and verifies dead-letter on type mismatch.

`hdata-sqs`:
- `SQSReadConfigTest` / `SQSWriteConfigTest`: config binding and validation (blank queue_url, out-of-range batch_size, etc.).
- `SQSSerializationTest`: ensures `SQSReadFn` and `SQSWriteFn` are serializable.

`hdata-prometheus`:
- `PrometheusReadConfigTest`: config binding and validation (blank endpoint, blank query, zero timeouts).
- `PrometheusSerializationTest`: ensures `PrometheusReadFn` is serializable.
- `PrometheusReadFnTest`: spins up a local `HttpServer` returning mock Prometheus JSON responses,
  runs the pipeline through `PrometheusReadFn`, and verifies parsed rows via `PAssert`. Covers single series,
  multiple series, and empty results.

`hdata-dynamodb`:
- `DynamoDBReadConfigTest` / `DynamoDBWriteConfigTest`: config binding and validation (blank table_name, out-of-range batch_size, etc.).
- `DynamoDBSerializationTest`: ensures `DynamoDBReadFn` and `DynamoDBWriteFn` are serializable.
- `DynamoDBContainerIT` (under `-Pintegration-tests`): `amazon/dynamodb-local` container; creates table,
  writes items via SDK, reads them back via the read provider, and verifies dead-letter on non-existent table.

A few invariants **that only hold if the tests are written correctly** — all are real bugs found during investigation; do not lose them when touching related code:
- `hdata-ftp`'s `when the split point lands exactly at a line start, that line must not be lost`: the split point must be a **whole multiple of the line length**,
  in order to cover rule 4 in the "all read sides use Splittable DoFn" section.
  The original test case used `size/3`, which always lands mid-line, so this bug was never exposed.
- `hdata-iceberg`'s `BYTES column round-trips without loss`: Iceberg's binary wants `ByteBuffer`, Beam's BYTES wants `ByteArray`,
  and the conversion in both directions was once written backwards, yet no test touched a BYTES column.
- `hdata-iceberg`'s `write_mode overwrite clears the table first`: go through the full provider chain,
  so the table-clear step (side input) is within coverage. Calling only the DoFn cannot test it.
- `hdata-neo4j`'s batching case: the assertion checks "whether it took the transaction path" (`txRuns` / `sessionRuns`),
  not how many transactions were committed — how DirectRunner splits bundles directly affects the commit count.
  Also the faked `DriverFactory` must be an `object` with a thread-safe counter: it must serialize alongside the DoFn,
  and the assertion looks at the in-process shared static state, while DirectRunner distributes bundles across multiple threads.

Behavior assertions run on DirectRunner (AssertEqual relies on the runner to execute assertions).

## Git history and commit messages
- Write every new commit message in English. Use a concise imperative subject, optionally prefixed with
  `feat:`, `fix:`, `docs:`, `test:`, `refactor:`, or `chore:`.
- Keep the subject line focused on the resulting change; do not include Chinese text, issue transcripts, or
  temporary debugging context in the commit message.

## Misc
- Package names are uniformly `me.jayer.hdata.*`.
- Logging uses log4j2, with default config at `hdata-core/src/main/resources/log4j2.xml`, overridable via `-Dlog4j2.configurationFile`.
