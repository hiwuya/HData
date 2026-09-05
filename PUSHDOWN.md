# 读取端下推能力对照表

对齐 Trino 的思路：能推给数据源算的，就不要把数据拉到 Beam worker 上再算。
下表是各读取连接器目前支持的**读取端下推**（在数据源侧生效，减少传输与计算）。

术语：
- **谓词下推**：过滤条件在数据源侧求值（`where` / `filter` / `scan_query`）。
- **投影下推**：只取需要的列，数据源侧少传数据。
- **limit**：最多读 N 行；全局语义（见下）。
- **聚合**：`count` / `min` / `max` / `sum` / `avg` 在数据源侧完成。
- **分区裁剪**：只扫与过滤条件相关的分区/分片。
- **并行读**：按某种维度把一次读取切成多份并行（Splittable DoFn）。

> **关于 limit 的全局语义**：凡是"按分区/分片并行读"的连接器，如果直接把 `limit` 下推到每个分片，
> 就会变成"每片各读 limit 条"而非"整结果集最多 limit 条"。因此 JDBC / MongoDB / Elasticsearch
> 只允许对单个表、集合或索引限行，并强制只生成一个读取单元；Iceberg 退化为单 worker，跨文件读取到
> 真正满足 N 行为止。

| 连接器 | 谓词下推 | 投影下推 | limit | 聚合下推 | 分区裁剪 | 并行读维度 |
|---|---|---|---|---|---|---|
| `ReadFromHive` | ✅ 分区裁剪 + 文件内谓词跳过（ORC/Parquet 行组/条纹级） | ✅ 只读声明/聚合涉及的列（ORC/Parquet 列裁剪） | ✅ 只扫 N 行 + sample | ✅ count/min/max/sum/avg，扫文件累加、跨文件归并 | ✅ 按分区列值只扫相关目录 | ✅ 按文件/列块（SDF） |
| `ReadFromJdbc` | ✅ `where` | ✅ `columns` | ✅ 单表 `LIMIT`，仅允许单分区 | ✅ count/min/max/sum/avg → DB 原生 `SELECT` 聚合（单行） | N/A（按列分区并行） | ✅ 按主键/数字列分区，探测失败退化单分区 |
| `ReadFromMongoDb` | ✅ `filter`（扩展 JSON） | ✅ `projection()` | ✅ 单集合 `find().limit()`，不与 `partition_num` 叠加 | ✅ count/sum/min/max/avg → 分片级局部 `$group` + 全局归并 | N/A | ✅ 按 `_id` 区间 `$bucketAuto` 分片 |
| `ReadFromElasticsearch` / `ReadFromElasticsearch6` | ✅ `scan_query`（ES 原生 DSL） | ✅ `_source` includes 投影（只取 `schema_fields` 声明的列，document 模式不下推） | ✅ 单索引、单 slice 扫到第 N 条停翻页 | ✅ count/min/max/sum/avg → ES 原生 aggregation（全局单行，不走 slice 并行） | N/A（按 slice 并行） | ✅ 按文档 ID 哈希 slice |
| `ReadFromKafka` | ❌（按 topic/分区消费，无谓词概念） | ❌ | ❌ | ❌ | N/A | ✅ 按分区 |
| `ReadFromHBase` | ❌（全表/按 start-end rowkey 范围） | ❌ | ❌ | ❌ | N/A（按 rowkey 范围） | ✅ `scan` 并行（`HBaseIO.readAll`） |
| `ReadFromFtp` / `ReadFromFilesystem` | ❌（整文件） | ❌ | ❌ | ❌ | N/A | ✅ 按字节区间 |
| `ReadFromRedis` | ❌（scan/keys/stream 模式，有界快照） | ❌ | ❌ | ❌ | N/A | ✅ 按 key/索引/触发元素 |
| `ReadFromNeo4j` | ❌（Cypher 在 driver 端构图，不连库） | ❌ | ❌ | ❌ | N/A | ⚠️ 一次性有界快照 |
| `ReadFromIceberg` | ✅ 谓词（manifest 级裁剪 + 读端 `Evaluator` 残留过滤） | ✅ 聚合/读取只取相关列（列投影） | ✅ 单 worker 跨文件读取，精确到 N 行 | ✅ count/min/max/sum/avg（投影列扫描 + 全局归并；sum/avg 按列累加，avg 用 sum/非空计数还原） | ✅ TableScan manifest 裁剪（普通读取） | ✅ 按 data file，大文件按 `split_size` 切同步块并行 |
| `ReadFromDebezium` | ❌（CDC 变更流） | ❌ | ❌ | ❌ | N/A | ✅ 按表/库并行嵌入式引擎 |

## 各下推的实现要点

### Hive
- 谓词：分区表先按分区列值做**分区裁剪**（只列相关分区目录），其余谓词下推到 ORC/Parquet 的
  行组/条纹级统计信息做**谓词跳过**。
- 投影：ORC/Parquet 支持列裁剪，只读取 `schema_fields` 声明的列；聚合下推时只读取聚合涉及的列。
- 聚合：`HiveAggregateFn` 在扫描每个文件时就地累加 count/sum/min/max，AVG 用 `sum/count` 的
  `sum#count` encoded 形式传出，跨文件在 Beam 侧归并（见 `HiveAggregateFn`）。
- limit/sample：下推成"只扫前 N 行"。

### JDBC
- `where` / `columns` 直接拼进 `SELECT`。`limit` 拼成 `LIMIT N`；限行只允许单表，拒绝
  `partition_column`，且 `partition_num` 只能留空或设为 1，以保证全局语义。
- 聚合：`aggregations` 配置（count/sum/min/max/avg）翻译成 DB 原生 `SELECT <agg> FROM <表|query> [WHERE ...]`，
  交由数据库侧算完返回单行（JDBC 形态本就是单语句读，聚合模式自然不再并行分区读、也不叠加 `columns`/`limit`）。
  聚合列别名用双引号包住锁定大小写（`COUNT(*) AS "count"`、`MIN(col) AS "min_col"`），与 Iceberg/MongoDB 命名一致；
  H2/Postgres 直接支持，MySQL 需开启 `ANSI_QUOTES`。聚合结果自带 schema（count→INT64，其余→DOUBLE），由 `aggregateSchema` 推导。
- 并行读按数值/主键列把表切成互不重叠的区间（`Sql.limit` 与分区探测见 `JdbcReadProvider`）。

### MongoDB
- `filter` 是扩展 JSON，直接作为 `find` 的查询；`projection()` 只取声明字段。
- `limit` 下推成 `find().limit(N)`；配置限行时拒绝 `partition_num`，保证只生成单个读取分片。
- 聚合：`aggregate` 配置（count/sum/min/max/avg）翻译成 MongoDB 聚合管道，但**分两阶段**——
  每个 `_id` 分片跑一次局部 `$group`（`_id:null`，产出 `count` 求和基数、`sum` 累加、`min/max` 最值、
  `avg` 的 `sum` 与非空计数），再由 Beam 侧 `MongoAggregateCombineFn` 跨分片全局归并、`MongoAggregateToRowFn`
  拼成最终一行（`avg = sum / 非空计数`）。这样既真下推到 MongoDB 计算，又能跨分片得到正确全局结果（等价 Trino 的
  "分片级局部聚合 + 最终合并"），而不是像以前那样强制单分片。空集合退化为单分片过滤，仍产出 `count=0` 的那一行。
  聚合结果自带 schema（count→INT64，其余→DOUBLE），由 `aggregateSchema` 推导。

### Elasticsearch
- `scan_query` 就是 ES 原生 Query DSL，天然是谓词下推。
- 投影：`schema_fields` 声明的字段通过 `_source` includes 下推（6.x `fetchSource(includes, null)`、
  8.x `source { filter { includes(...) } }`）；document 模式（`schema_fields` 为空）不下推，读整篇 `_source`。
- `limit`：PIT + search_after（8.x）/ scroll（6.x）翻页没有原生"全局 limit"；配置限行时只允许
  单索引并拒绝 `scan_slices`，同时把每页 `size` 压到剩余条数、扫到第 N 条停止翻页。
- 聚合：`aggregations` 配置（count/min/max/sum/avg）翻译成 ES 原生 aggregation（8.x `Aggregation.min{field}` 拼进
  `search.aggregations`；6.x `AggregationBuilders.min(field)` 拼进 `SearchSourceBuilder`），`size(0)` 一次查询让 ES 侧算完，
  结果返回**全局单行**（count 取 `hits.totalHits`/`track_total_hits`，数值聚合取 `value()`）。聚合是**全部配置索引**上的全局计算：
  所有索引合成一次查询、输出一行（不能按索引发元素——那会变成每索引一行的局部聚合）；所以不走 slice 并行，
  且不与 `schema_fields`/`limit`/`scan_slices` 叠加（配置校验会拒绝同时出现）。聚合结果自带 schema（count→INT64，其余→DOUBLE）。

### Iceberg
- 谓词：解析 `filter` 成 Iceberg `Expression`，先交给 `TableScan.filter(...)` 做 **manifest 级裁剪**
  （配合写入端补齐的数据文件 `lower/upper_bounds`，可整文件跳过）；读端再用 `Evaluator` 对每行
  （数据列 + 分区列）求一次残留谓词，保证下推真正生效、不静默漏过滤。
- 投影：聚合/读取只取相关列——聚合只投影 `min:/max:` 涉及的列，普通读取投影 `schema_fields` 声明列。
- `limit`：Iceberg 没有原生全局 LIMIT，限行数时退化为单 worker，使用 Iceberg `GenericReader`
  跨任意数量的数据文件顺序扫描，在过滤与 delete 文件生效后真正产出 `limit` 行才停止。
- 聚合：`count` 直接取文件元数据 `recordCount()`（不读数据）；`min/max/sum/avg` 只投影对应列逐文件扫描累加，
  跨文件在 `Combine.globally` 归并成一行（`AggregateCombineFn` + `AggregateToRowFn`）。`sum/avg` 没有数据文件级统计，
  但可以在读取端按列累加得到正确结果（`sum` 直接累加，`avg` 用 `sum/非空计数` 还原），min/max 与列同类型、sum/avg 统一为 DOUBLE。
  **带 `filter` 时聚合与普通读路径同一套谓词语义**：先 TableScan manifest 级裁剪，再对每行用 `Evaluator`
  求残留谓词后只累计匹配的行——COUNT 不能再拿整文件的 recordCount 充数（这里曾是一个作业成功但数字全错的 bug）。
  （Iceberg 1.10 的 `InternalData.write` 不会把列统计写进 manifest，故 `min/max` 走投影列扫描而非纯元数据。）
- 并行读：枚举 `TableScan.planFiles()` 的每个 `FileScanTask` 作为一个并行单元；AVRO 大文件按
  `split_size`（默认 128MB）切成同步块级 split 并行（`IcebergSplitEnumeratorFn`，不重不漏）。
- 写入端：每个 bundle 落盘时自己算一份数据文件统计（`Metrics` 的 `lower/upper_bounds`/`null_value_counts`）
  写进 `DataFile`，让上面谓词的 manifest 裁剪能真正跳过文件；`value_counts` 保留 Iceberg 规定的总行数语义，
  null 另记入 `null_value_counts`，避免混合空值文件被误判成全 NULL。
- 普通无限制并行读与聚合目前不支持 position/equality delete 文件，枚举阶段会明确失败；限行路径走
  Iceberg GenericReader，能正确应用 delete。静默返回已删除数据是不可接受的。

## 尚未覆盖、后续可做的下推
- Iceberg 分区表的分区裁剪（当前表未建分区）。

## 分片（sharding）实现审计：对标 Trino

目的：逐一核对各连接器的读取端并行分片实现是否对齐 Trino 的 split 思路
（splits 在规划期算好、互不重叠且覆盖全表），找出 bug 并优化。当前切分区间均有不重不漏测试；
本轮又修复了 JDBC 的 NULL 独立块参与数值均分、Hive 嵌套 schema 统计列错位，以及 Iceberg limit
只读首文件等不会主动报错的正确性问题。

### 各连接器分片实现与结论

| 连接器 | 分片维度 | 实现 | 对标 Trino | 结论 |
|---|---|---|---|---|
| `ReadFromHive` | 按文件 / 列块 | 自研 SDF，跨文件 + ORC stripe / Parquet row group / 文本行 | Trino Hive 按文件 + 内部 split | ✅ 正确；按行切分遵守 `from-1` 铁律（`TextRecordReader.kt:61`） |
| `ReadFromJdbc` | 按主键/数字列区间 | `boundary(index)=base+remainder` 均分，NULL 使用独立块 | Trino 按 min/max 均分 | ✅ 正确；已修类型上界与 NULL 块边界 bug |
| `ReadFromMongoDb` | 按 `_id` 区间 `$bucketAuto` | `MongoBuckets` 重建 `_id` 区间 | Trino Mongo `$bucketAuto` / 采样 | ✅ 正确 |
| `ReadFromElasticsearch` / `6` | 按 `_id` 哈希 slice | `effectiveSlices()`；限行数退化为单 slice | Trino ES 按 slice | ✅ 正确 |
| `ReadFromFtp` | 按字节区间 | 行边界校正、切分点取整行长度；排除 UTF-16 | Trino 无（整文件） | ✅ 正确 |
| `ReadFromFilesystem` | 按字节区间 | Beam `TextIO.readFiles()` | 同 | ✅ 正确（复用官方实现） |
| `ReadFromKafka` | 按 topic 分区 | Beam 官方 | Trino Kafka 按分区 | ✅ 正确（复用官方实现） |
| `ReadFromHBase` | 按 scan 并行 | Beam `HBaseIO.readAll()` | Trino HBase 按 region | ✅ 正确（复用官方实现） |
| `ReadFromIceberg` | 按 data file | 枚举 `FileScanTask` 逐个文件并行读（`IcebergSplitEnumeratorFn` + `IcebergReadFileFn`） | Trino 按 data file（再按格式块） | ✅ 已补文件级并行；当前 AVRO 大文件按同步块细分 |
| `ReadFromRedis`/`Neo4j`/`Debezium` | 非 SDF | 有界快照 / driver 端构图 / CDC 流 | 同形态 | ✅ 符合预期，非 SDF |

### 已修复的 bug

**JDBC 分区读最后一个查询块静默丢行**（`JdbcPartitionedReadFn`）
- 现象：分区列取值取到列类型上界（如 `INT = 2147483647`）时，最后一个块的上界
  `dataTo = toOffset(max) + 1 = 2147483648`，回灌成 `INT` 被 `PartitionConverter.fromLong`
  回绕成负数，于是 `col < 负数` 把边界那一行悄悄丢掉。
- 修复：边界之后本就没有更大的值，最后一个查询块只下推 `col >= ?`、不再带 `< ?` 上界
  （提交 `8375555`）。同时补 H2 边界值端到端回归用例，验证 `1` 与 `2147483647` 都读得到。

**JDBC NULL 查询块压缩数值区间**（`JdbcRestriction`）
- 现象：分区列同时有 NULL 与多个非 NULL 值时，总块数把末尾 NULL 块也算进数值边界的除数，最后一段
  数值区间没有任何查询负责；全 NULL 表则直接违反 restriction 构造不变量。
- 修复：tracker 的总块数与 `numericChunkCount` 分离，只有数值块参与边界均分；补混合 NULL/非 NULL
  和全 NULL 表的端到端回归。

**Hive 嵌套列之前的统计错位**（`HiveStatExtract`）
- ORC 顶层字段 ordinal 不等于扁平类型树 column id，Parquet 顶层 ordinal 也不等于 leaf column ordinal。
  旧实现遇到前置嵌套列会读取错误统计/向量，导致谓词误裁剪或聚合错误。
- 修复：统一解析 `OrcColumnRef` / `ParquetColumnRef`；Parquet INT64 统计同时改为直接转 BigDecimal，
  避免先过 Double 丢失 2^53 以上整数精度。

### 已知差距 / 后续优化

1. **Iceberg 文件级并行（已实现）**：`ReadFromIceberg` 枚举 `TableScan.planFiles()` 的 `FileScanTask`，
   每个数据文件作为一个并行单元；当前 AVRO 大文件再按 `split_size` 切同步块级 split 并行（提交 `db3bb3d`）。
2. **JDBC NULL 分区列（已实现）**：对齐 Trino 把 NULL 行放进独立 split（提交 `db93482`），不再 fail-loud。
3. **Elasticsearch 投影下推（已实现）**：`_source` includes 已按 `schema_fields` 下推（提交 `123953d`）。
4. **Iceberg 投影/谓词/limit/聚合下推（已实现）**：谓词走 manifest 裁剪 + 读端 `Evaluator` 残留过滤；
   limit 退化为单 worker 并跨文件精确读取；count/min/max/sum/avg 走投影列扫描 + 全局归并；
   写入端补齐数据文件列统计。
5. **JDBC 聚合下推（已实现）**：`aggregations` 翻译成 DB 原生 `SELECT <agg>` 聚合语句，数据库侧算完返回单行；
   count/sum/min/max/avg 全支持（提交见 `JdbcReadProvider` / `internal/AggregateSql.kt`）。
6. **Elasticsearch 6/8 聚合下推（已实现）**：`aggregations` 翻译成 ES 原生 aggregation（8.x `Aggregation`、6.x `AggregationBuilders`），
   `size(0)` 全局查询返回单行；count 取 `totalHits`、数值聚合取 `value()`（提交见 `EsAggregateFn` / `Elasticsearch6AggregateFn`）。
7. **Iceberg 聚合 sum/avg（已实现）**：原本因 AVRO 不含这两项统计而拒绝，现改为读取端按投影列累加
   （`sum` 直接累加、`avg` 用 `sum/非空计数` 还原），与 min/max 共享"逐文件局部聚合 + 全局归并"范式。
8. **MongoDB 聚合跨分片全局归并（已实现）**：`aggregate` 改为每个 `_id` 分片跑局部 `$group`、再 `MongoAggregateCombineFn`
   全局归并，不再强制单分片（提交见 `MongoPartialAggregateFn` / `MongoAggregate.kt`）。
