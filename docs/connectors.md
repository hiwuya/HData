# 连接器配置参数详细说明

本文档按连接器模块逐个列出 `Read*` / `Write*` 的 **全部** 配置参数（取值、默认值、约束/互斥关系）。
所有参数都写在 pipeline 节点 `config:` 下。

约定：

- **键名是 `snake_case`**，由框架按 SNAKE_CASE 命名策略绑定到 Kotlin 配置类的属性；
  `FAIL_ON_UNKNOWN_PROPERTIES` 是**开启**的，写错参数名或拼错键会直接报错，不会静默忽略。
- **`error_handling` 由框架摘走**，不进配置类；只有支持死信的 sink 才接受它（见文末《死信》一节）。
- **变量替换**在解析前做：`${MYSQL_PASSWORD}` 从环境变量 / `-D` 系统属性 / 默认值
  （`${MYSQL_PASSWORD:-root}`）取值，缺省且无默认值则构图失败。
- 布尔类型用 `true` / `false`；列表用 YAML 列表或行内 `[a, b]`；对象用 YAML 映射。

---

## 目录

- [通用：schema_fields 字段类型表](#schema_fields-字段类型表)
- [核心内置 transform](#核心内置-transform)
- [JDBC](#jdbc)
- [Kafka](#kafka)
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
- [死信（error_handling）](#死信error_handling)

---

## schema_fields 字段类型表

很多连接器用 `schema_fields: ["name:TYPE", ...]` 声明输出/输入列。各连接器的 TYPE 词表不同：

| 连接器 | 接受的类型（不区分大小写） |
|---|---|
| **JDBC** | 无 `schema_fields`；列来自源表的元数据 |
| **Hive** | 无 `schema_fields`；列来自 metastore |
| **Debezium** | 固定 schema（见该节），无 `schema_fields` |
| **MongoDB** | `STRING` `INT32` `INT64` `DOUBLE` `BOOLEAN` `DATETIME` `BYTES` |
| **HBase** | `STRING` `INT32` `INT64` `DOUBLE` `BOOLEAN` `BYTES` |
| **Kafka** | 格式而非类型：`string`（UTF-8→STRING）/ `raw`（原样 BYTES） |
| **Neo4j** | `STRING`；`INT64`/`BIGINT`/`LONG`；`INT32`/`INT`；`INT16`/`SMALLINT`/`SHORT`；`INT8`/`TINYINT`/`BYTE`；`FLOAT64`/`DOUBLE`；`FLOAT32`/`FLOAT`/`REAL`；`BOOLEAN`/`BOOL`；`BYTES`/`BINARY`/`BLOB` |
| **Iceberg** | `STRING`；`INT64`/`BIGINT`/`LONG`；`INT32`/`INT`；`FLOAT64`/`DOUBLE`；`FLOAT32`/`FLOAT`/`REAL`；`BOOLEAN`/`BOOL`；`BYTES`/`BINARY`/`BLOB`；`DATETIME`/`TIMESTAMP` |
| **Elasticsearch 6/8** | `STRING` `INT32` `INT64` `DOUBLE` `BOOLEAN` `DATETIME` `BYTES` |
| **FTP** | 小写：`string` `int`/`int32` `long`/`int64` `float` `double` `boolean`/`bool` |
| **Filesystem** | 小写：`string` `int`/`integer` `long` `float` `double` `boolean` `short` `byte` |

> 不写类型时，Neo4j / Iceberg 退化为 `STRING`；MySQL/HBase/FTP/Filesystem 的退化规则见各节。
> 省略 `schema_fields` 时，MongoDB / Elasticsearch 退化为单列 `document`（STRING，整条记录的 JSON），
> 读出来可直接写回。

---

## 核心内置 transform

这些随 `hdata-core` 提供，不算外部连接器，但常用。

| type | 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|---|
| `Create` | `elements` | list(object) | 必填（≥1） | 由字面量造数据，schema 由字面量推断；测试/联调用 |
| `AssertEqual` | `elements` | list(object) | 必填 | 断言输入恰好等于给定记录集合（顺序无关），原样透传；需 DirectRunner 这类支持断言的 runner |
| `LogForTesting` | `level` | string | `INFO` | `TRACE`/`DEBUG`/`INFO`/`WARN`/`ERROR` |
| `LogForTesting` | `prefix` | string | `""` | 每条记录前打印的前缀 |
| `MapToFields` | `fields` | map | `{}` | `目标字段: 源字段` 的映射（纯字段引用，无表达式） |
| `MapToFields` | `append` | bool | `false` | `true` 时保留输入全部字段再叠加 `fields` |
| `MapToFields` | `drop` | list | `[]` | 仅在 `append: true` 时生效，从保留字段里去掉这些名 |
| `Flatten` | — | — | — | 不接受 config；合并多路同 schema 输入，至少一路 |
| `StripErrorMetadata` | — | — | — | 不接受 config；把死信记录还原成原始记录，输入必须是死信流 |

`MapToFields` 约束：`fields` 非空，或 `append: true` 且 `drop` 非空；`drop` 必须与 `append` 同用。

---

## JDBC

读写共享连接字段（写在 `config` 顶层即可）：

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `url` | string | 必填 | JDBC URL，如 `jdbc:mysql://127.0.0.1:3306/demo` |
| `user` | string | `""` | |
| `password` | string | `""` | 建议用 `${MYSQL_PASSWORD}` 注入 |
| `driver_class` | string | `""` | 通常不用填，JDBC SPI 自动发现驱动 |
| `connection_properties` | map | `{}` | 透传给 HikariCP，如 `maximumPoolSize` |

### ReadFromJdbc

| 参数 | 类型 | 默认 | 说明 / 约束 |
|---|---|---|---|
| `tables` | list(string) | 必填* | 表名，支持区间写法 `t_order_${00-15}` |
| `query` | string | `""` | 直接给 SQL；一旦填写，`tables`/`where`/`partition_*` 全部失效 |
| `columns` | list(string) | `["*"]` | 列名，默认全列 |
| `where` | string | `""` | 附加过滤条件 |
| `partition_column` | string | `""` | 分区列；留空自动尝试主键 |
| `partition_num` | int? | `null` | 分片数，留空按取值范围估算（上限 10000） |
| `fetch_size` | int | `10000` | 每次往返取多少行 |

\* `tables` 与 `query` 至少填一个，且互斥。`query` 模式下 `columns` 必须为 `["*"]`。

### WriteToJdbc

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `table` | string | 必填 | 目标表 |
| `batch_size` | int | `10000` | 攒够多少行提交一次 |
| `retry_max_attempts` | int | `3` | 批量失败重试次数 |
| `retry_initial_seconds` | long | `3` | 重试退避基准 |
| `retry_max_seconds` | long | `60` | 重试退避上限（必须 ≥ `retry_initial_seconds`） |

---

## Kafka

对齐 Flink Kafka connector 的键名。读出行的固定 schema 为
`key` / `value` / `topic` / `partition` / `offset` / `timestamp` / `timestamp_type` / `headers`，
其中 `key`/`value` 类型由 `key_format`/`value_format` 决定。

### ReadFromKafka

| 参数 | 类型 | 默认 | 说明 / 约束 |
|---|---|---|---|
| `bootstrap_servers` | string | 必填 | 逗号分隔的 broker 列表 |
| `topics` | list(string) | `[]` | 要读的 topic；与 `topic_pattern` 二选一 |
| `topic_pattern` | string | `""` | topic 名正则；与 `topics` 互斥 |
| `group_id` | string | `""` | `group-offsets` 模式与 `commit_offsets_on_checkpoint` 需要 |
| `properties` | map | `{}` | 透传给 KafkaConsumer，如 `security.protocol` |
| `scan_startup_mode` | string | `earliest-offset` | `earliest-offset`/`latest-offset`/`group-offsets`/`specific-offsets`/`timestamp` |
| `scan_startup_specific_offsets` | map | `{}` | `scan_startup_mode=specific-offsets` 用，`"topic:partition" -> offset` |
| `scan_startup_timestamp_millis` | long? | `null` | `scan_startup_mode=timestamp` 用 |
| `scan_bounded_mode` | string | `latest-offset` | `unbounded`/`latest-offset`/`group-offsets`/`specific-offsets`/`timestamp`；**默认 `latest-offset`（有界快照），要流式消费显式写 `unbounded`** |
| `scan_bounded_specific_offsets` | map | `{}` | `scan_bounded_mode=specific-offsets` 用（不含该 offset） |
| `scan_bounded_timestamp_millis` | long? | `null` | `scan_bounded_mode=timestamp` 用 |
| `key_format` | string | `string` | `string`（UTF-8→STRING）/ `raw`（原样 BYTES） |
| `value_format` | string | `string` | 同上 |
| `commit_offsets_on_checkpoint` | bool | `false` | 读完后把偏移量提交回消费组（仅监控用，不影响 HData 容错）；需要 `group_id` |

`specific-offsets` / `timestamp` 相关的偏移量/时间戳参数只在对应模式生效，模式不对时配了会报错。

### WriteToKafka

| 参数 | 类型 | 默认 | 说明 / 约束 |
|---|---|---|---|
| `bootstrap_servers` | string | 必填 | |
| `topic` | string | `""` | 目标 topic；**留空则按输入行的 `topic` 字段路由**（可直接接 ReadFromKafka 做跨集群搬运） |
| `properties` | map | `{}` | 透传给 KafkaProducer |
| `batch_size` | int | `1000` | 攒够多少条 flush 并检查发送结果（也是最大在途条数） |
| `key_format` | string | `string` | 需与输入行 `key` 字段类型一致 |
| `value_format` | string | `string` | 需与输入行 `value` 字段类型一致 |
| `sink_delivery_guarantee` | string | `at-least-once` | `at-least-once`（acks=all）/`none`（acks=0）；**`exactly-once` 显式报错**，未实现 |

输入行需含 `value` 字段，`key` 可选。

---

## Hive

连的是 **metastore**（默认 9083），不是 HiveServer2（10000）；元数据从 metastore 拿，
数据直接读写表目录下的文件，与 Trino 的 Hive 连接器同路。表不存在则写端直接报错（不替你建表）。

### ReadFromHive

| 参数 | 类型 | 默认 | 说明 / 约束 |
|---|---|---|---|
| `metastore_uri` | string | 必填 | 如 `thrift://localhost:9083` |
| `database` | string | `default` | |
| `table` | string | 必填 | |
| `partitions` | list(string) | `[]` | 显式分区名 `dt=2024-01-01/hr=01`；与 `partition_filter` 互斥 |
| `partition_filter` | string | `""` | metastore 分区过滤表达式，如 `dt = '2024-01-01'` |
| `columns` | list(string) | `[]` | 只读这些列（含分区列）；空=全列（列式格式下少读磁盘） |
| `recursive_directories` | bool | `false` | 分区目录下还有子目录时是否递归 |
| `hadoop_conf` | map | `{}` | 透传 Hadoop `Configuration`，如 `fs.defaultFS`、对象存储 ak/sk |
| `metastore_timeout_millis` | int | `60000` | metastore socket 超时 |
| `split_bytes` | long | `67108864` (64MB) | 单分片最大字节数，仅对可切分格式有效 |

### WriteToHive

| 参数 | 类型 | 默认 | 说明 / 约束 |
|---|---|---|---|
| `metastore_uri` | string | 必填 | |
| `database` | string | `default` | |
| `table` | string | 必填 | 目标表须已存在 |
| `write_mode` | string | `append` | `append`（INSERT INTO）/ `overwrite`（INSERT OVERWRITE，**只清本次写到的分区**） |
| `create_partitions` | bool | `true` | 写完把新分区注册进 metastore；关掉则 Hive 查不到新分区 |
| `num_shards` | int | `0` | 落盘分片数，`0` 交给 runner |
| `file_prefix` | string | `part` | 文件名前缀 |
| `hadoop_conf` | map | `{}` | 同读端 |
| `metastore_timeout_millis` | int | `60000` | |

分区是**动态**的：每行按自己的分区列取值决定落点，一次作业可写任意多个分区。

---

## Redis

客户端用 Redisson。连接字段（读/写共享，写在 `config` 顶层）：

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `host` | string | `localhost` | |
| `port` | int | `6379` | |
| `password` | string | `""` | |
| `database` | int | `0` | |
| `ssl` | bool | `false` | |
| `timeout_ms` | int | `5000` | 连接/读写超时 |

### ReadFromRedis

| 参数 | 类型 | 默认 | 说明 / 约束 |
|---|---|---|---|
| `mode` | string | `scan` | `scan`/`keys`/`stream` |
| `key_pattern` | string | `*` | `mode=scan`：SCAN 匹配模式 |
| `keys` | list(string) | `[]` | `mode=keys`：要读的 key 列表 |
| `stream` | string | `""` | `mode=stream`：要读的 stream 名 |
| `start_id` | string | `-` | `mode=stream`：起始 entry id（`-`=头） |
| `end_id` | string | `+` | `mode=stream`：结束 entry id（`+`=尾） |

互斥：`mode=scan` 不用 `keys`/`stream`/`start_id`/`end_id`；`mode=keys` 不用 `stream`/`start_id`/`end_id`；
`mode=stream` 不用 `keys`/`key_pattern`。`stream` 模式每个条目按字段展开成多行（`id`/`field`/`value`）。

### WriteToRedis

| 参数 | 类型 | 默认 | 说明 / 约束 |
|---|---|---|---|
| `mode` | string | `set` | `set`/`lpush`/`rpush`/`sadd`/`hset` |
| `key_field` | string | `key` | 行里作为 Redis key 的字段名 |
| `value_field` | string | `value` | 行里作为 value 的字段名 |
| `hash_field` | string | `field` | `mode=hset` 时作为 hash 字段名的行字段 |
| `ttl_seconds` | long? | `null` | 过期秒数；`set` 用 SETEX，其余用 EXPIRE；不填不过期 |

---

## Neo4j

官方 Java Driver。连接字段（读/写共享）：

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `uri` | string | `bolt://localhost:7687` | |
| `user` | string | `neo4j` | |
| `password` | string | `""` | |
| `database` | string? | `null` | 留空用默认库 |

### ReadFromNeo4j

| 参数 | 类型 | 默认 | 说明 / 约束 |
|---|---|---|---|
| `query` | string | 必填 | Cypher，返回记录按 `schema_fields` 取字段 |
| `schema_fields` | list(`name:TYPE`) | 必填 | 输出 schema；不连库即可构图 |
| `parameters` | map | `null` | 常量查询参数 `name -> 值` |

### WriteToNeo4j

| 参数 | 类型 | 默认 | 说明 / 约束 |
|---|---|---|---|
| `statement` | string | 必填 | Cypher 写入语句，行字段按名绑定 `$param` |
| `parameters` | map | `null` | `cypher参数名 -> 行字段名`；不填则按同名自动绑定 |
| `batch_size` | int | `1000` | 这么多行一个事务提交；批量失败退回逐条写以定位坏数据 |

`parameters` 的键必须与 `statement` 里的 `$xxx` 占位符完全一致。

---

## Iceberg

HadoopCatalog，warehouse 是本地目录或 HDFS/S3 路径。读写共享：

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `warehouse` | string | 必填 | catalog 根目录 |
| `catalog_name` | string | `hdata` | |
| `table` | string | 必填 | 形如 `db.users` |
| `schema_fields` | list(`name:TYPE`) | 必填 | 见上文类型表（基础标量，不支持嵌套/list/map） |

### ReadFromIceberg

> 同上共享参数 + `schema_fields`（必填）。输出 schema 由 `schema_fields` 声明，不连库即可构图。

### WriteToIceberg

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `write_mode` | string | `append` | `append`（追加）/ `overwrite`（**所有写入前**原子清空表再写） |

表不存在则自动创建（无分区）。`overwrite` 的清表通过 side input 保证只做一次、且排在写入之前。

---

## Debezium

`ReadFromDebezium` 基于 Debezium 嵌入式引擎做 CDC，**只读**。输出 schema 固定：

| 字段 | 含义 |
|---|---|
| `op` | `c`(insert)/`u`(update)/`d`(delete)/`r`(snapshot)/`t`(truncate) |
| `key` | 主键（JSON 字符串） |
| `before` | 变更前整行（JSON，可能为 null） |
| `after` | 变更后整行（JSON，可能为 null） |
| `source` | 来源元数据（JSON） |
| `ts_ms` | 变更时间戳（毫秒） |

因 schema 固定，同一 pipeline 可捕获多张结构不同的表。

| 参数 | 类型 | 默认 | 说明 / 约束 |
|---|---|---|---|
| `connector` | string | `mysql` | `mysql`/`postgres`，或显式给 `connector_class` |
| `connector_class` | string? | `null` | 任意 Debezium 连接器类，优先级高于 `connector` |
| `host` | string? | `null` | 内置 mysql/postgres 必填 |
| `port` | int? | `null` | 1..65535 |
| `user` | string? | `null` | 内置 mysql/postgres 必填 |
| `password` | string? | `null` | |
| `database` | string? | `null` | postgres 必填 |
| `table_include` | string? | `null` | 表名过滤，如 `demo\\.t_user` |
| `snapshot_mode` | string? | `initial` | 透传 Debezium `snapshot.mode` |
| `server_name` | string? | `hdata` | 对应 Debezium `topic.prefix` |
| `server_id` | int? | `184054` | mysql 用 |
| `offset_file` | string? | 临时文件 | offset 存储文件 |
| `schema_history_file` | string? | 临时文件 | mysql schema 历史文件 |
| `name` | string? | `hdata-debezium` | 引擎名 |
| `max_records` | int? | `null` | 最多捕获多少条（测试用） |
| `extra` | map | `null` | 透传的额外引擎属性，优先级最高 |

---

## MongoDB

对齐 Flink MongoDB connector 键名。客户端官方 Java Driver。省略 `schema_fields` 时退化为
单列 `document`（STRING，整条文档的扩展 JSON），读出来可直接写回。

### ReadFromMongoDb

| 参数 | 类型 | 默认 | 说明 / 约束 |
|---|---|---|---|
| `connection_uri` | string | 必填 | 合法 MongoDB URI |
| `database` | string | 必填 | |
| `collection` | string | 必填 | |
| `schema_fields` | list(`name:TYPE`) | `[]` | 类型见上文表；空=单列 `document` |
| `filter` | string | `""` | 查询条件（MongoDB 查询 JSON），空=全量 |
| `partition_num` | int? | `null` | 并行分片数 1..1000；留空按文档数估算 |
| `fetch_size` | int | `1000` | 游标每次往返取多少条 |

### WriteToMongoDb

| 参数 | 类型 | 默认 | 说明 / 约束 |
|---|---|---|---|
| `connection_uri` | string | 必填 | |
| `database` | string | 必填 | |
| `collection` | string | 必填 | |
| `schema_fields` | list(`name:TYPE`) | `[]` | 空=按输入 `document`(STRING) 字段写 |
| `upsert_keys` | list(string) | `[]` | 按这些字段覆盖写；键必须在 `schema_fields` 内；空=一律 insert（重跑会产生重复） |
| `batch_size` | int | `1000` | 攒够多少行提交一次 |

---

## HBase

扫描复用 Beam 的 `HBaseIO.readAll()`（`read()` 是旧 `BoundedSource`，不用）。HBase 类型比 Beam 更宽，
定长类型（INT32/INT64/DOUBLE/BOOLEAN）遇到宽度对不上的单元格会直接报错而非静默截断。

### ReadFromHBase

| 参数 | 类型 | 默认 | 说明 / 约束 |
|---|---|---|---|
| `zookeeper_quorum` | string | 必填 | 如 `localhost:2181` |
| `zookeeper_znode_parent` | string | `""` | |
| `table` | string | 必填 | |
| `rowkey_field` | string | `rowkey` | 输出行里行键的字段名 |
| `rowkey_format` | string | `string` | `string`/`bytes`；**二进制 rowkey 必须用 `bytes`，否则被 UTF-8 破坏** |
| `family` | string | `cf` | `schema_fields` 里没写列族的条目默认落在此列族 |
| `schema_fields` | list | 必填 | `qualifier:type` 或 `family:qualifier:type`；为空则全表拉列，报错 |
| `scan_start_row` | string | `""` | 起始 rowkey（含）；空=从头 |
| `scan_stop_row` | string | `""` | 结束 rowkey（不含）；空=到尾 |
| `scan_caching` | int | `100` | 每次 RPC 拉多少行；全表扫描默认关块缓存 |
| `scan_cache_blocks` | bool | `false` | 是否进 RegionServer 块缓存；全表扫描默认关 |
| `properties` | map | `{}` | 透传 HBase 属性 |

### WriteToHBase

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `zookeeper_quorum` | string | 必填 | |
| `zookeeper_znode_parent` | string | `""` | |
| `table` | string | 必填 | |
| `rowkey_field` | string | `rowkey` | 需与输入行 rowkey 字段类型一致 |
| `rowkey_format` | string | `string` | `string`/`bytes` |
| `family` | string | `cf` | |
| `schema_fields` | list | 必填 | 空则每行只写出空 Put |
| `batch_size` | int | `1000` | 攒够多少行提交一次 |
| `properties` | map | `{}` | |

---

## FTP

按**字节区间**并行读（靠 `REST` 定位起点），写是分片的（每个并行单元产 `<file_prefix>-<分片号><扩展名>`）。

### ReadFromFtp

| 参数 | 类型 | 默认 | 说明 / 约束 |
|---|---|---|---|
| `host` / `host_name` | string | 必填 | 二者别名，不能取不同值 |
| `port` | int | `21` | 1..65535 |
| `user` / `username` | string | `""` | 别名 |
| `password` | string | `""` | |
| `path` | string | 必填 | 远程目录或单个文件 |
| `file_pattern` | string? | `null` | 可选 glob，如 `*.csv` |
| `file_format` | string | `text` | `text`（固定单列 `content`）/`csv` |
| `schema_fields` | list | `null` | `csv` 必填；`text` 不可用 |
| `header` | bool | `false` | `csv` 首行是否表头；`text` 不可用 |
| `encoding` | string | `UTF-8` | 合法字符集 |
| `csv_delimiter` | string | `,` | 单字符；`text` 不可用 |
| `csv_quote` | string | `"` | 单字符；`text` 不可用 |
| `timeout_millis` | int | `30000` | 连接/读写超时 |

### WriteToFtp

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `host`/`host_name` | string | 必填 | |
| `port` | int | `21` | |
| `user`/`username` | string | `""` | |
| `password` | string | `""` | |
| `path` | string | 必填 | 远程目录 |
| `file_prefix` | string | `hdata-output` | 文件名前缀 |
| `file_format` | string | `text` | `text`/`csv` |
| `schema_fields` | list | `null` | `csv` 必填 |
| `header` | bool | `false` | `csv` 写表头 |
| `encoding` | string | `UTF-8` | |
| `csv_delimiter` | string | `,` | 单字符 |
| `csv_quote` | string | `"` | 单字符 |
| `batch_size` | int | `1000` | 攒够多少行 flush |
| `timeout_millis` | int | `30000` | |

---

## Filesystem

走 Beam `FileSystems`，由 `path` 的 scheme 决定文件系统。`text` 用 `TextIO.readFiles()`（真字节区间切分）。

### ReadFromFilesystem

| 参数 | 类型 | 默认 | 说明 / 约束 |
|---|---|---|---|
| `path` | string | 必填 | 目录或通配符，如 `file:///tmp/input/all-*.csv`、`hdfs://ns/data` |
| `default_fs` | string | `file:///` | 仅 `path` 无 scheme 时用作基准；必须带 scheme |
| `file_format` | string | `text` | `text`/`csv`/`xlsx` |
| `schema_fields` | list | `[]` | `csv`/`xlsx` 必填；`text` 不可用 |
| `header` | bool | `false` | `csv`/`xlsx` 跳过首行；`text` 不可用 |
| `sheet` | string | `""` | `xlsx` 工作表名，空=第一个；其余格式不可用 |
| `encoding` | string | `UTF-8` | `text`/`xlsx` 固定 UTF-8 |
| `csv_delimiter` | string | `,` | 单字符；仅 `csv` 用 |
| `csv_quote` | string | `"` | 单字符；仅 `csv` 用 |

### WriteToFilesystem

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `path` | string | 必填 | 输出**目录** |
| `default_fs` | string | `file:///` | 必须带 scheme |
| `file_format` | string | `text` | `text`/`csv`/`xlsx` |
| `schema_fields` | list | `[]` | `csv`/`xlsx` 必填 |
| `header` | bool | `false` | `csv`/`xlsx` 写表头 |
| `sheet` | string | `""` | `xlsx` 工作表名 |
| `encoding` | string | `UTF-8` | `text`/`xlsx` 固定 UTF-8 |
| `csv_delimiter` | string | `,` | 单字符 |
| `csv_quote` | string | `"` | 单字符 |
| `file_prefix` | string | `output` | 文件名前缀 |
| `num_shards` | int | `0` | 分片数，`0`=runner 决定；**`xlsx` 必须 `1`**（一个工作簿即一个 zip 容器） |

---

## Elasticsearch 6

### ReadFromElasticsearch6

| 参数 | 类型 | 默认 | 说明 / 约束 |
|---|---|---|---|
| `connection_uri` | string | 必填 | 逗号分隔的 http/https 节点 |
| `index` | string | `""` | 单索引；与 `indices` 二选一 |
| `indices` | list(string) | `[]` | 多索引；与 `index` 互斥 |
| `username` | string | `""` | |
| `password` | string | `""` | |
| `schema_fields` | list(`name:TYPE`) | `[]` | 空=单列 `document` |
| `scan_query` | string | `""` | 查询 DSL JSON，空=全量 |
| `scroll_size` | int | `1000` | 每次 scroll 翻页条数 |
| `scroll_timeout_minutes` | long | `1` | scroll 上下文存活时间 |
| `scan_slices` | int | `1` | 按文档 ID 哈希切几片并行读，建议等于分片数 |

### WriteToElasticsearch6

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `connection_uri` | string | 必填 | |
| `index` | string | 必填 | |
| `username` | string | `""` | |
| `password` | string | `""` | |
| `schema_fields` | list(`name:TYPE`) | `[]` | 空=按输入 `value`(STRING) 当原始 JSON 写 |
| `batch_size` | int | `1000` | |

---

## Elasticsearch 8

与 6 的区别：读端用 **PIT**（point in time）替代 scroll，多一个 `keep_alive_minutes`。

### ReadFromElasticsearch8

| 参数 | 类型 | 默认 | 说明 / 约束 |
|---|---|---|---|
| `connection_uri` | string | 必填 | 逗号分隔的 http/https 节点 |
| `index` | string | `""` | 单索引；与 `indices` 二选一 |
| `indices` | list(string) | `[]` | 多索引；与 `index` 互斥 |
| `api_key` | string | `""` | 鉴权用 api key（与 user/password 二选一） |
| `username` | string | `""` | |
| `password` | string | `""` | |
| `schema_fields` | list(`name:TYPE`) | `[]` | 空=单列 `document` |
| `batch_size` | int | `1000` | 每次翻页条数 |
| `scan_query` | string | `""` | 查询 DSL JSON，空=match_all |
| `scan_slices` | int | `1` | 并行切片数，建议等于分片数 |
| `keep_alive_minutes` | int | `5` | PIT 存活时间（单 slice 两次翻页间不能超过） |

### WriteToElasticsearch8

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `connection_uri` | string | 必填 | |
| `index` | string | 必填 | |
| `api_key` | string | `""` | |
| `username` | string | `""` | |
| `password` | string | `""` | |
| `schema_fields` | list(`name:TYPE`) | `[]` | 空=按输入 `value`(STRING) 当原始 JSON 写 |
| `batch_size` | int | `1000` | |

---

## 死信（error_handling）

写在 **sink 的 `config`** 里，由框架摘走再交给配置类绑定。语法对齐 Beam YAML：

```yaml
config:
  error_handling:
    output: errors        # 死信流以 <节点名>.errors 暴露给 DAG，且必须被下游消费
```

- `output`（必填）：死信流标签名，外部引用写作 `WriteOrders.errors`（见 `examples/dead-letter.yaml`）。
- 声明后**必须**有下游消费它，否则构图阶段直接报错（避免"以为兜住了其实丢了"）。
- `threshold`（错误率阈值）：**暂未实现**，一旦声明会在构图阶段报错，不会静默失效。

死信记录 schema 为 `{element: ROW<原始记录>, error_type, error_message, transform}`，
可用 `StripErrorMetadata` 剥掉元信息还原成原始记录。死信记录带**原始行自己的时间戳与窗口**，
可直接 `LogForTesting` 或写回备份表。
