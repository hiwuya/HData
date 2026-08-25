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
> 就会变成"每片各读 limit 条"而非"整结果集最多 limit 条"。所以限行数时一律**退化为单分区/单分片读**，
> 让 `limit` 在源头对整个结果集生效。JDBC / MongoDB / Elasticsearch 都是这个策略。

| 连接器 | 谓词下推 | 投影下推 | limit | 聚合下推 | 分区裁剪 | 并行读维度 |
|---|---|---|---|---|---|---|
| `ReadFromHive` | ✅ 分区裁剪 + 文件内谓词跳过（ORC/Parquet 行组/条纹级） | ✅ 只读声明/聚合涉及的列（ORC/Parquet 列裁剪） | ✅ 只扫 N 行 + sample | ✅ count/min/max/sum/avg，扫文件累加、跨文件归并 | ✅ 按分区列值只扫相关目录 | ✅ 按文件/列块（SDF） |
| `ReadFromJdbc` | ✅ `where` | ✅ `columns` | ✅ `LIMIT`，限行数退化为单分区 | ❌ 暂未做 | N/A（按列分区并行） | ✅ 按主键/数字列分区，探测失败退化单分区 |
| `ReadFromMongoDb` | ✅ `filter`（扩展 JSON） | ✅ `projection()` | ✅ `find().limit()`，限行数退化为单分片 | ✅ count/sum/min/max/avg → `$group`+`$match`，强制单分片 | N/A | ✅ 按 `_id` 区间 `$bucketAuto` 分片 |
| `ReadFromElasticsearch` / `ReadFromElasticsearch6` | ✅ `scan_query`（ES 原生 DSL） | ✅ `_source` includes 投影（只取 `schema_fields` 声明的列，document 模式不下推） | ✅ 限行数退化为单 slice + 扫到第 N 条停翻页 | ❌ 暂未做 | N/A（按 slice 并行） | ✅ 按文档 ID 哈希 slice |
| `ReadFromKafka` | ❌（按 topic/分区消费，无谓词概念） | ❌ | ❌ | ❌ | N/A | ✅ 按分区 |
| `ReadFromHBase` | ❌（全表/按 start-end rowkey 范围） | ❌ | ❌ | ❌ | N/A（按 rowkey 范围） | ✅ `scan` 并行（`HBaseIO.readAll`） |
| `ReadFromFtp` / `ReadFromFilesystem` | ❌（整文件） | ❌ | ❌ | ❌ | N/A | ✅ 按字节区间 |
| `ReadFromRedis` | ❌（scan/keys/stream 模式，有界快照） | ❌ | ❌ | ❌ | N/A | ✅ 按 key/索引/触发元素 |
| `ReadFromNeo4j` | ❌（Cypher 在 driver 端构图，不连库） | ❌ | ❌ | ❌ | N/A | ⚠️ 一次性有界快照 |
| `ReadFromIceberg` | ✅ 谓词（manifest 级裁剪 + 读端 `Evaluator` 残留过滤） | ✅ 聚合/读取只取相关列（列投影） | ✅ 限行数退化为单文件（源头掐前 N 行） | ✅ count/min/max（投影列扫描 + 全局归并），sum/avg 被拒绝 | N/A（当前未做分区表） | ✅ 按 data file，大文件按 `split_size` 切 row-group 并行 |
| `ReadFromDebezium` | ❌（CDC 变更流） | ❌ | ❌ | ❌ | N/A | ✅ 按表/库并行嵌入式引擎 |

## 各下推的实现要点

### Hive
- 谓词：分区表先按分区列值做**分区裁剪**（只列相关分区目录），其余谓词下推到 ORC/Parquet 的
  行组/条纹级统计信息做**谓词跳过**。
- 投影：ORC/Parquet 支持列裁剪，只读取 `schema_fields` 声明的列；聚合下推时只读取聚合涉及的列。
- 聚合：`HiveAggregateFn` 在扫描每个文件时就地累加 count/sum/min/max，AVG 用 `sum/sum` 的
  `sum#count` encoded 形式传出，跨文件在 Beam 侧归并（见 `HiveAggregateFn`）。
- limit/sample：下推成"只扫前 N 行"。

### JDBC
- `where` / `columns` 直接拼进 `SELECT`。`limit` 拼成 `LIMIT N`；限行数时强制单语句（单分区），
  保证全局语义。
- 并行读按数值/主键列把表切成互不重叠的区间（`Sql.limit` 与分区探测见 `JdbcReadProvider`）。

### MongoDB
- `filter` 是扩展 JSON，直接作为 `find` 的查询；`projection()` 只取声明字段。
- `limit` 下推成 `find().limit(N)`，限行数时退化为单分片。
- 聚合：`aggregate` 配置（count/sum/min/max/avg）翻译成 `[$match filter, $group {...}]` 管道，
  强制单分片（不与 `_id` 分区并行读叠加），`_id:null` 得到整表聚合；空集合补 `count=0`、其余 null 的一行。
  聚合结果自带 schema（count→INT64，其余→DOUBLE），由 `aggregateSchema` 推导。

### Elasticsearch
- `scan_query` 就是 ES 原生 Query DSL，天然是谓词下推。
- 投影：`schema_fields` 声明的字段通过 `_source` includes 下推（6.x `fetchSource(includes, null)`、
  8.x `source { filter { includes(...) } }`）；document 模式（`schema_fields` 为空）不下推，读整篇 `_source`。
- `limit`：PIT + search_after（8.x）/ scroll（6.x）翻页没有原生"全局 limit"，
  限行数时退化为单 slice，并把每页 `size` 压到剩余条数、扫到第 N 条停止翻页。

### Iceberg
- 谓词：解析 `filter` 成 Iceberg `Expression`，先交给 `TableScan.filter(...)` 做 **manifest 级裁剪**
  （配合写入端补齐的数据文件 `lower/upper_bounds`，可整文件跳过）；读端再用 `Evaluator` 对每行
  （数据列 + 分区列）求一次残留谓词，保证下推真正生效、不静默漏过滤。
- 投影：聚合/读取只取相关列——聚合只投影 `min:/max:` 涉及的列，普通读取投影 `schema_fields` 声明列。
- `limit`：Iceberg 没有原生全局 LIMIT，且读按文件并行，限行数时**退化为单文件**（枚举只发第一个 split），
  读端在单文件内掐到 `limit` 行停止。
- 聚合：`count` 直接取文件元数据 `recordCount()`（不读数据）；`min/max` 只投影对应列逐文件扫描求最值，
  跨文件在 `Combine.globally` 归并成一行（`AggregateCombineFn` + `AggregateToRowFn`）。
  `sum/avg` 被显式拒绝——AVRO 数据文件不含这两项统计，收了又不生效等于埋坑。
  （Iceberg 1.10 的 `InternalData.write` 不会把列统计写进 manifest，故 `min/max` 走投影列扫描而非纯元数据。）
- 并行读：枚举 `TableScan.planFiles()` 的每个 `FileScanTask` 作为一个并行单元；大文件按 `split_size`
  （默认 128MB）切成多个 row-group 级 split 并行（`IcebergSplitEnumeratorFn`，不重不漏）。
- 写入端：每个 bundle 落盘时自己算一份数据文件统计（`Metrics` 的 `lower/upper_bounds`/`null_value_counts`）
  写进 `DataFile`，让上面谓词的 manifest 裁剪能真正跳过文件。

## 尚未覆盖、后续可做的下推
- JDBC / Elasticsearch 的聚合下推（count/min/max/sum/avg）。
- 跨分片的聚合全局归并（目前 MongoDB 聚合为求全局语义强制单分片，未做分片级局部聚合 + 最终合并；
  Iceberg 已做"逐文件局部聚合 + 全局归并"，是正确范式）。
- Iceberg 分区表的分区裁剪（当前表未建分区）。

## 分片（sharding）实现审计：对标 Trino

目的：逐一核对各连接器的读取端并行分片实现是否对齐 Trino 的 split 思路
（splits 在规划期算好、互不重叠且覆盖全表），找出 bug 并优化。结论：**自研 SDF 全部正确，
仅 JDBC 分区读有一处类型上界回绕的真实数据丢失 bug（已修），Iceberg 缺文件级并行是最大的差距点。**

### 各连接器分片实现与结论

| 连接器 | 分片维度 | 实现 | 对标 Trino | 结论 |
|---|---|---|---|---|
| `ReadFromHive` | 按文件 / 列块 | 自研 SDF，跨文件 + ORC stripe / Parquet row group / 文本行 | Trino Hive 按文件 + 内部 split | ✅ 正确；按行切分遵守 `from-1` 铁律（`TextRecordReader.kt:61`） |
| `ReadFromJdbc` | 按主键/数字列区间 | `boundary(index)=base+remainder` 均分，互不重叠 | Trino 按 min/max 均分 | ✅ 正确；⚠️ 已修"类型上界行丢失"bug |
| `ReadFromMongoDb` | 按 `_id` 区间 `$bucketAuto` | `MongoBuckets` 重建 `_id` 区间 | Trino Mongo `$bucketAuto` / 采样 | ✅ 正确 |
| `ReadFromElasticsearch` / `6` | 按 `_id` 哈希 slice | `effectiveSlices()`；限行数退化为单 slice | Trino ES 按 slice | ✅ 正确 |
| `ReadFromFtp` | 按字节区间 | 行边界校正、切分点取整行长度；排除 UTF-16 | Trino 无（整文件） | ✅ 正确 |
| `ReadFromFilesystem` | 按字节区间 | Beam `TextIO.readFiles()` | 同 | ✅ 正确（复用官方实现） |
| `ReadFromKafka` | 按 topic 分区 | Beam 官方 | Trino Kafka 按分区 | ✅ 正确（复用官方实现） |
| `ReadFromHBase` | 按 scan 并行 | Beam `HBaseIO.readAll()` | Trino HBase 按 region | ✅ 正确（复用官方实现） |
| `ReadFromIceberg` | 按 data file | 枚举 `FileScanTask` 逐个文件并行读（`IcebergSplitEnumeratorFn` + `IcebergReadFileFn`） | Trino 按 data file（再按 row group） | ✅ 已补文件级并行；大文件按 row group 细分未做 |
| `ReadFromRedis`/`Neo4j`/`Debezium` | 非 SDF | 有界快照 / driver 端构图 / CDC 流 | 同形态 | ✅ 符合预期，非 SDF |

### 已修复的 bug

**JDBC 分区读最后一个查询块静默丢行**（`JdbcPartitionedReadFn`）
- 现象：分区列取值取到列类型上界（如 `INT = 2147483647`）时，最后一个块的上界
  `dataTo = toOffset(max) + 1 = 2147483648`，回灌成 `INT` 被 `PartitionConverter.fromLong`
  回绕成负数，于是 `col < 负数` 把边界那一行悄悄丢掉。
- 修复：边界之后本就没有更大的值，最后一个查询块只下推 `col >= ?`、不再带 `< ?` 上界
  （提交 `8375555`）。同时补 H2 边界值端到端回归用例，验证 `1` 与 `2147483647` 都读得到。

### 已知差距 / 后续优化

1. **Iceberg 文件级并行（已实现）**：`ReadFromIceberg` 枚举 `TableScan.planFiles()` 的 `FileScanTask`，
   每个数据文件作为一个并行单元；大文件再按 `split_size` 切 row-group 级 split 并行（提交 `db3bb3d`）。
2. **JDBC NULL 分区列（已实现）**：对齐 Trino 把 NULL 行放进独立 split（提交 `db93482`），不再 fail-loud。
3. **Elasticsearch 投影下推（已实现）**：`_source` includes 已按 `schema_fields` 下推（提交 `123953d`）。
4. **Iceberg 投影/谓词/limit/聚合下推（已实现）**：谓词走 manifest 裁剪 + 读端 `Evaluator` 残留过滤；
   limit 退化为单文件；count/min/max 走投影列扫描 + 全局归并；写入端补齐数据文件列统计。sum/avg 被拒绝。
5. **Iceberg 聚合 sum/avg**：AVRO 数据文件不含这两项统计，当前显式拒绝；若要支持需在读取端按列累加
   （与 Hive 的 `HiveAggregateFn` 思路一致），代价是必须读数据，不再是纯统计下推。
