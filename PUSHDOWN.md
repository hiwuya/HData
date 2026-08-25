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
| `ReadFromElasticsearch` / `ReadFromElasticsearch6` | ✅ `scan_query`（ES 原生 DSL） | ⚠️ 由 `schema_fields` 在客户端取字段，未下推 `_source` includes | ✅ 限行数退化为单 slice + 扫到第 N 条停翻页 | ❌ 暂未做 | N/A（按 slice 并行） | ✅ 按文档 ID 哈希 slice |
| `ReadFromKafka` | ❌（按 topic/分区消费，无谓词概念） | ❌ | ❌ | ❌ | N/A | ✅ 按分区 |
| `ReadFromHBase` | ❌（全表/按 start-end rowkey 范围） | ❌ | ❌ | ❌ | N/A（按 rowkey 范围） | ✅ `scan` 并行（`HBaseIO.readAll`） |
| `ReadFromFtp` / `ReadFromFilesystem` | ❌（整文件） | ❌ | ❌ | ❌ | N/A | ✅ 按字节区间 |
| `ReadFromRedis` | ❌（scan/keys/stream 模式，有界快照） | ❌ | ❌ | ❌ | N/A | ✅ 按 key/索引/触发元素 |
| `ReadFromNeo4j` | ❌（Cypher 在 driver 端构图，不连库） | ❌ | ❌ | ❌ | N/A | ⚠️ 一次性有界快照 |
| `ReadFromIceberg` | ❌（整表快照读） | ❌ | ❌ | ❌ | N/A | ✅ 按 data file（每个数据文件一个并行单元，分区列从 split 回填） |
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
- 投影未下推 `_source` includes（目前 `schema_fields` 只决定客户端取哪些字段）。
- `limit`：PIT + search_after（8.x）/ scroll（6.x）翻页没有原生"全局 limit"，
  限行数时退化为单 slice，并把每页 `size` 压到剩余条数、扫到第 N 条停止翻页。

## 尚未覆盖、后续可做的下推
- JDBC / Elasticsearch 的聚合下推（count/min/max/sum/avg）。
- Elasticsearch 的 `_source` includes 投影下推。
- 跨分片的聚合全局归并（目前 MongoDB 聚合为求全局语义强制单分片，未做分片级局部聚合 + 最终合并）。

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

1. **Iceberg 文件级并行（已实现）**：`ReadFromIceberg` 现在枚举 `TableScan.planFiles()` 的
   `FileScanTask`，每个数据文件作为一个并行单元（`IcebergSplitEnumeratorFn` 枚举 split、
   `IcebergReadFileFn` 按文件读，分区列的值从 split 回填完整 schema）。读单文件走与写入端对称的
   `InternalData.read`，只投影数据列（分区列不存进数据文件）。尚未做的是大文件再按 row group 细分
   （Trino 会做），对当前固定写 AVRO、单文件通常不大的场景足够。
2. **JDBC NULL 分区列**：当前 `requireNoNulls` 在遇到分区列 NULL 时**显式报错**（有意的
   fail-loud，避免静默漏行，对应测试 `分区列上有 NULL 时拒绝执行`）。Trino 会把 NULL 行放进
   一个独立 split。若希望对齐 Trino 行为，可增加一个 `IS NULL` 的额外 chunk；但当前 fail-loud
   是刻意选择，改动需评估是否违背"宁可报错让用户显式处理"的约定。
3. **Elasticsearch 投影下推**：`schema_fields` 只在客户端挑字段，未下推 `_source` includes，
   传输量偏多（非分片问题，列在下推章节）。
4. **Iceberg 投影/谓词下推**：当前按文件读时投影到"全表数据列"，未下推到 `schema_fields` 声明的
   字段子集；谓词/limit/聚合也未下推。可后续在 `InternalData.read(...).project(请求子集)` 与
   `filter(...)` 上补齐（与 Hive/JDBC 思路一致）。
