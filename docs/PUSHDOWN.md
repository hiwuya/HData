# Read-side push-down capability matrix

Aligned with Trino's philosophy: if something can be computed by the data source, do not pull the data to a Beam worker to compute it.
The table below shows the **read-side push-down** currently supported by each read connector (taking effect on the source side, reducing transfer and computation).

Terms:
- **Predicate push-down**: filter conditions are evaluated on the source side (`where` / `filter` / `scan_query`).
- **Projection push-down**: only fetch the needed columns, so the source side transfers less data.
- **limit**: read at most N rows; global semantics (see below).
- **Aggregation**: `count` / `min` / `max` / `sum` / `avg` computed on the source side.
- **Partition pruning**: scan only the partitions/shards related to the filter condition.
- **Parallel read**: split one read into multiple pieces along some dimension for parallelism (Splittable DoFn).

> **On the global semantics of limit**: any connector that reads "in parallel by partition/shard" would, if it pushed `limit` down to each shard,
> end up with "each shard reads limit rows" rather than "the whole result set is at most limit rows". Therefore JDBC / MongoDB / Elasticsearch
> only allow row-limiting on a single table, collection, or index, and force a single read unit; Iceberg degrades to a single worker, reading across files until
> it truly satisfies N rows.

| Connector | Predicate push-down | Projection push-down | limit | Push-down aggregation | Partition pruning | Parallel-read dimension |
|---|---|---|---|---|---|---|
| `ReadFromHive` | ✅ partition pruning + in-file predicate skipping (ORC/Parquet stripe/row-group level) | ✅ read only columns in declarations/aggregations (ORC/Parquet column pruning) | ✅ scan only N rows + sample | ✅ count/min/max/sum/avg, accumulate across files then merge across files | ✅ scan only relevant directories by partition-column value | ✅ by file / column block (SDF) |
| `ReadFromJdbc` | ✅ `where` | ✅ `columns` | ✅ single-table `LIMIT`, single partition only | ✅ count/min/max/sum/avg → DB-native `SELECT` aggregation (single row) | N/A (parallel by column partition) | ✅ by primary-key/numeric column partition, degrades to single partition on detection failure |
| `ReadFromMongoDb` | ✅ `filter` (extended JSON) | ✅ `projection()` | ✅ single-collection `find().limit()`, not stacked with `partition_num` | ✅ count/sum/min/max/avg → per-shard local `$group` + global merge | N/A | ✅ by `_id` range via `$bucketAuto` sharding |
| `ReadFromElasticsearch` / `ReadFromElasticsearch6` | ✅ `scan_query` (ES-native DSL) | ✅ `_source` includes projection (only reads columns declared in `schema_fields`; no push-down in document mode) | ✅ single index, single slice, stop paging at the Nth row | ✅ count/min/max/sum/avg → ES-native aggregation (global single row, no slice parallelism) | N/A (parallel by slice) | ✅ by document-ID hash slice |
| `ReadFromKafka` | ❌ (consumes by topic/partition, no predicate concept) | ❌ | ❌ | ❌ | N/A | ✅ by partition |
| `ReadFromHBase` | ❌ (full table / by start-end rowkey range) | ❌ | ❌ | ❌ | N/A (by rowkey range) | ✅ `scan` parallelism (`HBaseIO.readAll`) |
| `ReadFromFtp` / `ReadFromFilesystem` | ❌ (whole file) | ❌ | ❌ | ❌ | N/A | ✅ by byte range |
| `ReadFromRedis` | ❌ (scan/keys/stream mode, bounded snapshot) | ❌ | ❌ | ❌ | N/A | ✅ by key/index/triggered element |
| `ReadFromNeo4j` | ❌ (Cypher graphed on driver side, no DB connection) | ❌ | ❌ | ❌ | N/A | ⚠️ one-shot bounded snapshot |
| `ReadFromIceberg` | ✅ predicate (manifest-level pruning + residual filtering via read-side `Evaluator`) | ✅ aggregations/reads fetch only relevant columns (column projection) | ✅ single worker reads across files, precise to N rows | ✅ count/min/max/sum/avg (projected-column scan + global merge; sum/avg accumulated by column, avg restored from sum/non-null count) | ✅ TableScan manifest pruning (normal read) | ✅ by data file, large files split into sync blocks by `split_size` for parallelism |
| `ReadFromDebezium` | ❌ (CDC change stream) | ❌ | ❌ | ❌ | N/A | ✅ parallel embedded engine by table/database |

## Implementation notes per push-down

### Hive
- Predicate: for partitioned tables, first do **partition pruning** by partition-column value (list only relevant partition directories); other predicates are pushed down to ORC/Parquet's
  stripe/row-group-level statistics for **predicate skipping**.
- Projection: ORC/Parquet support column pruning, reading only columns declared in `schema_fields`; during aggregation push-down, only the columns involved in aggregation are read.
- Aggregation: `HiveAggregateFn` accumulates count/sum/min/max in place while scanning each file; AVG is carried out in the `sum#count` encoded form of `sum/count`,
  then merged across files on the Beam side (see `HiveAggregateFn`).
- limit/sample: pushed down as "scan only the first N rows".

### JDBC
- `where` / `columns` are concatenated directly into `SELECT`. `limit` becomes `LIMIT N`; row-limiting is allowed only on a single table and rejects
  `partition_column`, and `partition_num` may only be empty or set to 1, to guarantee global semantics.
- Aggregation: the `aggregations` config (count/sum/min/max/avg) translates to a DB-native `SELECT <agg> FROM <table|query> [WHERE ...]`,
  handed to the database to compute and return a single row (the JDBC shape is inherently a single-statement read; aggregation mode naturally no longer reads in parallel partitions, nor stacks `columns`/`limit`).
  Aggregate-column aliases are wrapped in double quotes to lock case (`COUNT(*) AS "count"`, `MIN(col) AS "min_col"`), consistent with Iceberg/MongoDB naming;
  H2/Postgres support it directly, MySQL needs `ANSI_QUOTES` enabled. The aggregate result carries its own schema (count→INT64, the rest→DOUBLE), derived by `aggregateSchema`.
- Parallel read splits the table into non-overlapping ranges by numeric/primary-key column (see `JdbcReadProvider` for Sql.limit and partition detection).

### MongoDB
- `filter` is extended JSON, used directly as the `find` query; `projection()` fetches only the declared fields.
- `limit` is pushed down as `find().limit(N)`; when row-limiting is configured, `partition_num` is rejected to guarantee a single read shard.
- Aggregation: the `aggregate` config (count/sum/min/max/avg) translates to a MongoDB aggregation pipeline, but **in two phases** —
  each `_id` shard runs a local `$group` once (`_id:null`, producing the `count` cardinality sum, `sum` accumulation, `min/max` extremes,
  and `avg`'s `sum` and non-null count), then Beam-side `MongoAggregateCombineFn` merges globally across shards and `MongoAggregateToRowFn`
  assembles the final single row (`avg = sum / non-null count`). This genuinely pushes computation to MongoDB while yielding correct global results across shards (equivalent to Trino's
  "per-shard local aggregation + final merge") instead of forcing a single shard as before. An empty collection degrades to a single-shard filter, still producing the `count=0` row.
  The aggregate result carries its own schema (count→INT64, the rest→DOUBLE), derived by `aggregateSchema`.

### Elasticsearch
- `scan_query` is native ES Query DSL, inherently a predicate push-down.
- Projection: fields declared in `schema_fields` are pushed down via `_source` includes (6.x `fetchSource(includes, null)`,
  8.x `source { filter { includes(...) } }`); document mode (empty `schema_fields`) does no push-down, reading the whole `_source`.
- `limit`: PIT + search_after (8.x) / scroll (6.x) paging has no native "global limit"; when row-limiting is configured, only a single index is allowed and `scan_slices` is rejected,
  while each page's `size` is pressed down to the remaining count and paging stops at the Nth row.
- Aggregation: the `aggregations` config (count/min/max/sum/avg) translates to ES-native aggregation (8.x `Aggregation.min{field}` plugged into
  `search.aggregations`; 6.x `AggregationBuilders.min(field)` plugged into `SearchSourceBuilder`). A single `size(0)` query lets ES compute it all,
  returning a **global single row** (count from `hits.totalHits`/`track_total_hits`, numeric aggregation from `value()`). The aggregation is a **global computation over all configured indexes**:
  all indexes are combined into one query and output one row (it must not emit per-index elements — that would become a per-index local aggregation); so no slice parallelism,
  and it is not stacked with `schema_fields`/`limit`/`scan_slices` (config validation rejects their simultaneous appearance). The aggregate result carries its own schema (count→INT64, the rest→DOUBLE).

### Iceberg
- Predicate: parse `filter` into an Iceberg `Expression`, first handed to `TableScan.filter(...)` for **manifest-level pruning**
  (together with the `lower/upper_bounds` of data files filled in by the write side, whole files can be skipped); then the read side uses `Evaluator` to evaluate the residual predicate once per row
  (data columns + partition columns), guaranteeing the push-down truly takes effect and does not silently miss filtering.
- Projection: aggregations/reads fetch only relevant columns — aggregation projects only columns involved in `min:`/`max:`, normal reads project columns declared in `schema_fields`.
- `limit`: Iceberg has no native global LIMIT; when row-limiting, it degrades to a single worker using Iceberg's `GenericReader`
  to scan sequentially across any number of data files, stopping only after filtering and delete files have taken effect and it truly produces `limit` rows.
- Aggregation: `count` takes the file metadata's `recordCount()` directly (no data read); `min/max/sum/avg` project only the corresponding columns and accumulate per file,
  then merge into one row across files in `Combine.globally` (`AggregateCombineFn` + `AggregateToRowFn`). `sum/avg` have no data-file-level statistics,
  but can be accumulated by column on the read side for correct results (`sum` accumulates directly, `avg` restored from `sum/non-null count`); min/max are the same type as the column, sum/avg are unified to DOUBLE.
  **With `filter`, aggregation and the normal read path share the same predicate semantics**: first TableScan manifest-level pruning, then per-row residual predicate via `Evaluator`,
  accumulating only matching rows — COUNT can no longer pad with the whole file's recordCount (this was once a bug where the job succeeded but all numbers were wrong).
  (Iceberg 1.10's `InternalData.write` does not write column statistics into the manifest, so `min/max` goes through projected-column scanning rather than pure metadata.)
- Parallel read: enumerate each `FileScanTask` of `TableScan.planFiles()` as one parallel unit; AVRO large files are split by
  `split_size` (default 128MB) into sync-block-level splits for parallelism (`IcebergSplitEnumeratorFn`, no overlap, no miss).
- Write side: when each bundle writes to disk it computes a set of data-file statistics itself (`Metrics`' `lower/upper_bounds`/`null_value_counts`)
  and writes them into `DataFile`, so the predicate's manifest pruning above can truly skip files; `value_counts` preserves Iceberg's required total-row semantics,
  nulls are separately recorded in `null_value_counts` to avoid a mixed-null file being misjudged as all NULL.
- Normal unlimited parallel read and aggregation do not yet support position/equality delete files and will explicitly fail at enumeration; the row-limiting path goes through
  Iceberg's GenericReader, which correctly applies deletes. Silently returning deleted data is unacceptable.

## Not yet covered, future push-downs
- Iceberg partition-table partition pruning (the current table has no partitions built).

## Sharding implementation audit: against Trino

Purpose: verify one by one whether each connector's read-side parallel sharding aligns with Trino's split philosophy
(splits computed at planning time, non-overlapping and covering the whole table), find bugs, and optimize. Current split ranges all have no-overlap/no-miss tests;
this round also fixed JDBC's NULL-independent-block participation in numeric averaging, Hive's nested-schema statistics-column misalignment, and Iceberg limit
only-reading-the-first-file — correctness problems that would not proactively error.

### Per-connector sharding implementation and conclusion

| Connector | Shard dimension | Implementation | vs Trino | Conclusion |
|---|---|---|---|---|
| `ReadFromHive` | by file / column block | self-built SDF, cross-file + ORC stripe / Parquet row group / text line | Trino Hive by file + internal split | ✅ correct; line-based split follows the `from-1` iron rule (`TextRecordReader.kt:61`) |
| `ReadFromJdbc` | by primary-key/numeric-column range | `boundary(index)=base+remainder` even split, NULL uses a separate block | Trino by min/max even split | ✅ correct; fixed type upper-bound and NULL-block boundary bug |
| `ReadFromMongoDb` | by `_id` range `$bucketAuto` | `MongoBuckets` rebuilds `_id` range | Trino Mongo `$bucketAuto` / sampling | ✅ correct |
| `ReadFromElasticsearch` / `6` | by `_id` hash slice | `effectiveSlices()`; row-limiting degrades to single slice | Trino ES by slice | ✅ correct |
| `ReadFromFtp` | by byte range | line-boundary correction, split point rounded to whole line length; excludes UTF-16 | Trino none (whole file) | ✅ correct |
| `ReadFromFilesystem` | by byte range | Beam `TextIO.readFiles()` | same | ✅ correct (reuses official implementation) |
| `ReadFromKafka` | by topic partition | Beam official | Trino Kafka by partition | ✅ correct (reuses official implementation) |
| `ReadFromHBase` | by scan parallelism | Beam `HBaseIO.readAll()` | Trino HBase by region | ✅ correct (reuses official implementation) |
| `ReadFromIceberg` | by data file | enumerate `FileScanTask` for per-file parallel read (`IcebergSplitEnumeratorFn` + `IcebergReadFileFn`) | Trino by data file (then by format block) | ✅ file-level parallelism added; current AVRO large files subdivided by sync block |
| `ReadFromRedis`/`Neo4j`/`Debezium` | non-SDF | bounded snapshot / driver-side graph / CDC stream | same shape | ✅ as expected, non-SDF |

### Bugs fixed

**JDBC partitioned read silently drops rows in the last query block** (`JdbcPartitionedReadFn`)
- Symptom: when the partition column value reaches the column type's upper bound (e.g. `INT = 2147483647`), the upper bound of the last block
  `dataTo = toOffset(max) + 1 = 2147483648`, cast back to `INT` and wrapped negative by `PartitionConverter.fromLong`,
  so `col < negative` silently drops the boundary row.
- Fix: beyond the boundary there are no larger values, so the last query block only pushes `col >= ?` and no longer carries the `< ?` upper bound
  (commit `8375555`). Also added an H2 end-to-end boundary-value regression case, verifying that both `1` and `2147483647` are read.

**JDBC NULL query block compresses the numeric range** (`JdbcRestriction`)
- Symptom: when the partition column has both NULL and multiple non-NULL values, the total block count counts the trailing NULL block into the divisor of the numeric boundary,
  so the last numeric range has no query responsible for it; an all-NULL table directly violates the restriction-construction invariant.
- Fix: the tracker's total block count is separated from `numericChunkCount`, and only numeric blocks participate in boundary averaging; added end-to-end regressions for mixed NULL/non-NULL
  and all-NULL tables.

**Hive statistics misalignment before nested columns** (`HiveStatExtract`)
- The ORC top-level field ordinal does not equal the flattened type tree's column id, and the Parquet top-level ordinal does not equal the leaf column ordinal either.
  The old implementation, encountering a preceding nested column, would read the wrong statistics/vector, causing false predicate pruning or aggregation errors.
- Fix: uniformly parse `OrcColumnRef` / `ParquetColumnRef`; Parquet INT64 statistics also switched to convert directly to BigDecimal,
  avoiding the loss of integer precision above 2^53 that would come from going through Double first.

### Known gaps / future optimizations

1. **Iceberg file-level parallelism (implemented)**: `ReadFromIceberg` enumerates the `FileScanTask` of `TableScan.planFiles()`,
   each data file as one parallel unit; current AVRO large files further split by `split_size` into sync-block-level parallelism (commit `db3bb3d`).
2. **JDBC NULL partition column (implemented)**: aligned with Trino, puts NULL rows into a separate split (commit `db93482`), no longer fail-loud.
3. **Elasticsearch projection push-down (implemented)**: `_source` includes pushed down by `schema_fields` (commit `123953d`).
4. **Iceberg projection/predicate/limit/aggregation push-down (implemented)**: predicate goes through manifest pruning + read-side `Evaluator` residual filtering;
   limit degrades to single worker and reads precisely across files; count/min/max/sum/avg go through projected-column scanning + global merge;
   write side fills in data-file column statistics.
5. **JDBC aggregation push-down (implemented)**: `aggregations` translates to DB-native `SELECT <agg>` aggregation statements, computed on the DB side and returned as a single row;
   count/sum/min/max/avg all supported (see `JdbcReadProvider` / `internal/AggregateSql.kt`).
6. **Elasticsearch 6/8 aggregation push-down (implemented)**: `aggregations` translates to ES-native aggregation (8.x `Aggregation`, 6.x `AggregationBuilders`),
   `size(0)` global query returns a single row; count from `totalHits`, numeric aggregation from `value()` (see `EsAggregateFn` / `Elasticsearch6AggregateFn`).
7. **Iceberg aggregation sum/avg (implemented)**: previously rejected because AVRO lacks these two statistics; now the read side accumulates by projected column
   (`sum` directly accumulated, `avg` restored from `sum/non-null count`), sharing the "per-file local aggregation + global merge" paradigm with min/max.
8. **MongoDB aggregation global merge across shards (implemented)**: `aggregate` changed so each `_id` shard runs a local `$group`, then `MongoAggregateCombineFn`
   merges globally, no longer forcing a single shard (see `MongoPartialAggregateFn` / `MongoAggregate.kt`).
