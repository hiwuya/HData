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
| `ReadFromIceberg` | ❌（整表快照读） | ❌ | ❌ | ❌ | N/A | ⚠️ 按 data file |
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
