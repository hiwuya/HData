# AGENTS.md

HData —— 基于 Apache Beam 的数据同步/ETL 工具，Kotlin 编写，作业配置用 YAML，对齐 Beam YAML 规范。

## 构建环境
- 使用 **JDK 25** 构建（目标字节码 Java 25，Kotlin `jvmTarget` 已设为 25）。Kotlin 需 **>= 2.x** 才能在 JDK 25 上运行，当前为 2.4.10；降到 1.8.x 会崩溃（`IllegalArgumentException: 25.0.3`）。
- 使用 Maven 4（`mvn` 命令，**无 maven wrapper**）；依赖走 `~/.m2/settings.xml` 里的 aliyun 镜像，首次构建需联网。
- 根 pom 未标 `<root="true">`，Maven 4 会打印 "Unable to find the root directory" 警告，可忽略。
- Jackson 用 **3.x**（包名 `tools.jackson.*`），注解仍是 `com.fasterxml.jackson.annotation`。注意 Jackson 3 的 API 改名：`isTextual`→`isString`、`asText`→`asString`、`fieldNames`→`propertyNames`。

## 模块结构
- `hdata-core`：核心引擎与程序入口。`me.jayer.hdata.core.HData` 的 `main` 是唯一入口。
  - `spec/`：pipeline 文件的语法树、YAML 解析、变量替换、结构校验。**只支持 YAML**（TOML/JSON 已移除）。
  - `spi/`：连接器扩展点。`HDataTransform` 继承 Beam 的 `SchemaTransform`，即 `PCollectionRowTuple -> PCollectionRowTuple`。
  - `registry/`：`type` -> provider 注册表，同时桥接 classpath 上的 Beam 原生 `SchemaTransformProvider`。
  - `graph/`：语法树 -> Beam DAG，处理 chain/composite、引用解析、拓扑排序、死信、窗口。
  - `transforms/`：内置 transform（Create / MapToFields / Flatten / LogForTesting / StripErrorMetadata / AssertEqual）。
- `hdata-jdbc`：JDBC 连接器，`ReadFromJdbc` / `WriteToJdbc`。
  - `internal/`：实现细节。`TypeMappings` 是**不可变**的规则表，每列只解析一次，
    解析结果随 DoFn 序列化下发——往里加东西时注意**不要捕获普通 Kotlin lambda**，
    捕获 `Function1` 会让整个 DoFn 无法序列化（用 `ValueConverter` 这类可序列化 fun interface）。
  - `partition/`：分区列的选取与校验。`transform/`：三个 DoFn，连接池一律 `@Setup` 建、`@Teardown` 关。
- `hdata-kafka`：`ReadFromKafka` / `WriteToKafka`。读取**直接复用 Beam 的 `ReadFromKafkaDoFn`**，
  本模块只负责 `internal/KafkaOffsets`：把 Flink 风格的 startup/bounded 模式翻译成每分区的起止偏移量。
- `hdata-hive`：`ReadFromHive` / `WriteToHive`。**不走 JDBC / HiveServer2**，
  分层照搬 Trino 的 Hive 连接器：从 metastore 拿元数据，直接读写表目录下的数据文件。
  - `metastore/`：`HiveMetastore` 接口 + 不可变元数据模型。生产实现 `ThriftHiveMetastore` 直接说
    metastore 的 thrift 协议（用 IDL 生成的 `ThriftHiveMetastore.Client`，不是 `HiveMetaStoreClient`：
    后者会把 derby / grpc / curator / zookeeper 一整套服务端依赖拖进来，而我们只用 6 个只读调用）。
    测试实现 `InMemoryHiveMetastore` 按名字全局缓存，`memory://<名字>` 启用，**只在单个 JVM 内有效**。
    `PartitionNames` 是分区名的编解码，规则逐字对齐 Hive 的 `FileUtils.escapePathName` / `makePartName`。
  - `type/`：Hive 类型字符串的递归下降解析（嵌套类型不能按逗号 split）与值换算。
    `timestamp` 映射到 `DATETIME`（墙上时间）、`timestamp with local time zone` 才映射到 `TIMESTAMP`。
  - `split/`：列目录、判定可切分性、路径归一化。
  - `format/`：八种存储格式的读写器，读取器统一实现 `HiveRecordReader`（见下面的 SDF 一节）。
    `rcfile/` 下的 RCFile 容器格式与 LazyBinary 单值编码是**自己按格式规范实现**的——
    `RCFile` 那个类躺在 80MB 的 hive-exec 里，而那个 jar 把 avro / orc / parquet / protobuf
    各打了一份没重定位的副本进去。已验证与 Hive 双向兼容（见测试一节）。
  - `transform/`：列文件的 DoFn、按字节区间读的 SDF、行转记录、分区注册。
- `hdata-mongodb`：`ReadFromMongoDb` / `WriteToMongoDb`。`internal/MongoBuckets` 用 `$bucketAuto` 求 `_id`
  分桶边界，读取按桶下标切分；写入走 `bulkWrite`，支持 `upsert_keys`。
- `hdata-hbase`：`ReadFromHBase` / `WriteToHBase`。扫描**复用 Beam 的 `HBaseIO.readAll()`**
  （注意不是 `HBaseIO.read()`，后者内部还是老的 `BoundedSource`）。`HBaseRowCodec` 管行编解码。
- `hdata-ftp`：`ReadFromFtp` / `WriteToFtp`。按**字节区间**并行读，靠 FTP 的 `REST` 命令定位起点；
  写入是分片的，每个 bundle 先写 `.tmp` 再 `rename`。
- `hdata-filesystem`：`ReadFromFilesystem` / `WriteToFilesystem`。匹配用 `FileIO.match()`，
  text 读取用 `TextIO.readFiles()`（真正的字节区间切分），落盘用 `FileIO.write()`（分片 + 原子改名）。
- `hdata-elasticsearch-6` / `hdata-elasticsearch-8`：按 ES 原生 **slice** 并行读（`scan_slices`）。
- `hdata-redis`：`ReadFromRedis` / `WriteToRedis`，客户端用 Redisson。读支持 `scan` / `keys` / `stream`
  三种模式，key 的收集（SCAN / XRANGE）在 driver 端一次性做完（有界快照），再逐条取值；
  `stream` 模式的 `start_id` / `end_id` 按 `-` / `+` / `<毫秒>-<序号>` 解析，真的会限定 XRANGE 的区间。
- `hdata-neo4j`：`ReadFromNeo4j` / `WriteToNeo4j`，官方 Java Driver。读端**不连库即可构图**
  （输出 schema 由 `schema_fields` 声明）；写端执行 Cypher，行字段按名绑定成 `$param`，
  `batch_size` 行一个事务提交，批量失败退回逐条写以定位坏数据（同 `WriteToJdbc`）。
- `hdata-iceberg`：`ReadFromIceberg` / `WriteToIceberg`，HadoopCatalog。写端表不存在则自动建，
  每个 bundle 落一个 AVRO 数据文件再提交；`write_mode` 见下面的 Iceberg 一节。
- `hdata-debezium`：`ReadFromDebezium`，基于 Debezium 嵌入式引擎的 CDC 源。输出 schema 固定
  （`op` / `key` / `before` / `after` / `source` / `ts_ms`），所以一个 pipeline 可以同时捕获多张结构不同的表。

配置类只依赖 `TransformConfig.bind(...)`（Jackson 3），不要自己 new `YAMLMapper`；
写路径用 `@Setup`/`@FinishBundle`/`@Teardown` 管资源，失败行经 `ErrorSchemas.failure(...)` 进死信。

## 读取端一律用 Splittable DoFn（重要）

**能复用 Beam 官方 IO 的就不要自己写**：Kafka / HBase / Filesystem 已经换成官方实现（见上面的模块说明）。
其余模块（JDBC / Hive / MongoDB / Elasticsearch / FTP）Beam 没有 SDF 版实现，是自写的。

Redis / Neo4j / Iceberg / Debezium **不是 SDF**，别照着下面四条去改它们：前三个是在 driver 端
或单个 DoFn 里一次性把数据取完的有界快照（并行度来自 key / 索引 / 触发元素的个数），
Debezium 则是嵌入式引擎推数据进队列。要给它们加并行读的话，是**先设计切分维度**，
再按下面的规矩写 SDF，而不是把现有 DoFn 直接换个基类。

自己写 SDF 时有四条铁律，都是这轮重构里踩出来的：

1. **不要 `tryClaim(range.to - 1)` 一次性认领整段**。那等于告诉 Beam"这段不可再分"，
   运行时既没法把剩下的活分给空闲 worker，也拿不到进度。正确写法是循环里逐个认领。
   认领的粒度按格式各自最小的可定位单位来：Hive 这边 ORC 是 stripe、Parquet 是 row group、
   Avro / SequenceFile / RCFile 是同步块、文本是行。
2. **`checkDone()` 有契约**：`OffsetRangeTracker` 要求最后一次*尝试*的偏移量 >= `to - 1`。
   提前读完（比如文件到了 EOF）要补一次 `tryClaim(range.to)`——它返回 false 但记下这次尝试，
   否则报 `claiming work in [x, y) was not attempted`。
   反过来，**认领被拒绝之后不能再补**，否则会因为"认领的偏移量小于上一次尝试"直接抛异常，
   所以 `HiveRecordReader.read()` 用返回值区分"读完了"和"被拒了"。
   同理，认领的偏移量必须**严格递增**：块压缩的 SequenceFile 里连续多行的 `getPosition()` 是同一个值，
   要等位置真的前进了再认领。
3. **有些格式天生不能按字节切**：带引号的 CSV 字段可以内嵌换行，从任意字节位置切开会把记录劈成两半；
   xlsx 是 zip 容器只能从头解析；整文件 gzip / snappy 压过的文本只能从头解压；
   配了 `skip.header.line.count > 1` 或 `skip.footer.line.count` 的表也必须整文件读。
   这些的并行度来自文件个数，代码里要写清楚为什么。
4. **按行切分要从 `from - 1` 开始读，不是 `from`**。恰好有一行从 `from` 开始时，
   `from - 1` 上就是上一行的换行符，先 `readLine()` 一次只会吃掉那个换行符，这一行仍归本分片；
   直接从 `from` 读再丢掉第一行的话，这一整行会**凭空消失**——上一个分片在位置到达 `from` 时就停了，
   也不会读它。Beam 自己的 `TextSource` 就是这么处理的。

## Hive 连接器的边界与坑

支持的存储格式（读写双向，按 metastore 上的 SerDe + InputFormat 判定，判不出来**直接抛**，不猜）：
`TEXTFILE` / `CSV`(OpenCSVSerde) / `SEQUENCEFILE` / `RCTEXT` / `RCBINARY` / `ORC` / `PARQUET` / `AVRO`。

明确**不支持**、且会在构图阶段就报错的：
- **事务表（ACID）**：目录里是 `delta_*` / `base_*` 加行级增删改标记，按文件直接读会读出已经删掉的行；
- **视图**；`uniontype`；
- `RCBINARY` 下的嵌套类型（array / map / struct）——LazyBinary 的嵌套编码没有真实样本可验证，
  与其写一份没把握的实现，不如让用户换 ORC/Parquet。`RCTEXT` 走文本编码，嵌套类型正常支持。

两处**格式本身**的信息丢失，不是实现的问题，但要知道：
- `RCBINARY` 的 0 长度单元格既可能是 NULL 也可能是空串，读出来一律 NULL（Hive 自己也是）；
- `CSV` 里空字段同样分不开，所以写入端把 null 写成 `\N` 而不是空串。

写入端要求**目标表已存在**（建表是 DDL，不该由同步作业代劳）；表用什么格式就按什么格式写，
分区值来自行里的分区列、编码进目录名，**数据文件里不含分区列**，写完再把新分区注册进 metastore
（不注册的话文件在目录里躺着但 Hive 查不到）。

分区一律是**动态**的：每行按自己的分区列取值决定落到哪个分区，一次作业写出任意多个分区，
与 Hive 的动态分区插入一致。上游没有分区列时用 `MapToFields` 补一个常量列，不另做静态分区配置。
`write_mode` 决定已有数据怎么办：
- `append`（默认）= `INSERT INTO`，新文件加进去，旧文件不动；
- `overwrite` = `INSERT OVERWRITE`，**只清掉本次写到的那些分区**里的旧文件；
  没有数据落到的分区不动，这与 Hive 动态分区覆盖的语义一致。想清空整张表请自己 DROP。

追加写入这里有个必须知道的坑：Beam 的 `FileIO.Write.defaultNaming` 只按
`前缀-分片号-of-总数` 命名，**不带任何作业标识**，同一张表跑两次会生成一模一样的文件名，
第二次直接把第一次的结果盖掉——既不是追加也不是覆盖，而且作业状态还是成功。
所以文件名里必须掺一个每次作业唯一的标记（`HiveSink.runToken()`），
`HiveWriteModeTest` 专门钉了这条。

覆盖不是原子的：新文件已经改名到位、旧文件还没删完的那一小段时间里，读的人会同时看到两份数据。
没有 ACID 的 Hive 表本来就是这样（Hive 自己也一样），要强一致只能上事务表——而事务表这里不支持。

依赖上有三个只在运行时才炸的坑，动 pom 前先看这里：
1. `orc-core` 要用 `shaded-protobuf` 分类器，**而且必须显式声明同样 shaded 的 `orc-format`**——
   orc-core 的 pom 里 orc-format 是不带分类器的，混用会在写 stripe 时报
   `NoSuchMethodError: OrcProto$StripeFooter.writeTo(org.apache.orc.protobuf.CodedOutputStream)`。
2. `parquet-hadoop` 的代码路径会走到 `org.apache.hadoop.mapreduce.lib.input.FileInputFormat`，
   少了 `hadoop-mapreduce-client-core` 编译期毫无征兆，运行时 `NoClassDefFoundError`。
3. 写入端把落盘交给 Beam 的 `FileIO`，所以要带上 `beam-sdks-java-io-hadoop-file-system`，
   否则写 HDFS 上的表会报 "No filesystem found for scheme hdfs"。

另外，metastore 给的 location 是带 scheme 的 URI，交给 Beam `FileIO.write()` 之前必须过
`HivePaths.forBeamIO`：Beam 的 `LocalFileSystem` 对 `file://` 的读写行为不一致，
写的时候会把整个字符串当相对路径，数据落到当前工作目录下一个叫 `file:` 的目录里，**作业状态还是 DONE**。
`hdata-filesystem` 的 `FilesystemPaths` 是同一个坑。

## Iceberg 的 write_mode

`append`（默认）就是每个 bundle 落一个数据文件再 `newAppend().commit()`。

`overwrite` 是"先把表清空，再写本次的数据"，清表**必须恰好做一次、且排在所有写入之前**。
这件事不能放进写入端的 `@Setup`：那样第二个 bundle 会把第一个 bundle 刚写进去的数据删掉，
结果"覆盖"只剩最后一个 bundle 的内容，而作业状态还是成功。
所以清表是独立的 `IcebergTruncateFn`，通过 **side input** 挂在写入的 ParDo 上——
带 side input 的 ParDo 在 side input 那条 PCollection 完全算完之前不会处理任何主输入，
顺序由此保证。改这块之前先想清楚这一点。

清表本身用 `newDelete().deleteFromRowFilter(alwaysTrue())` 一次原子提交，不是自己删文件：
读的人要么看到旧快照要么看到空表，不会读到删了一半的中间状态；空表上再删一次是 no-op，
所以 bundle 重试是安全的。

两个已知边界：写出的数据文件固定是 AVRO，不跟随表自身的 write format；
类型只支持基础标量（见 `internal/IcebergSchemas`），嵌套 / list / map 还没做。

## 运行
- `me.jayer.hdata.core.HData --pipeline=<文件>`，另有 `--dryRun`（只构图打印）、`--waitUntilFinish`。
- pipeline 文件顶层是 `pipeline:` + 可选的 `options:`；`pipeline` 本身就是一个 composite/chain 形态的 transform 节点。
- 样例见 `examples/`；设计说明见 `ARCHITECTURE.md`。
- **Runner 是 Maven profile**：默认只有 DirectRunner，另有 `-Pflink-runner`（beam-runners-flink-2.2）
  与 `-Pspark-runner`（beam-runners-spark-4），两者都已在 JDK 25 上验证可跑通端到端作业。
  历史上 runner 依赖排除了引擎自身的 jar，会让 `PipelineOptionsFactory` 静态初始化直接失败，别再那么写。
- Spark 能跑在 JDK 25 上靠三处配合，动其中任何一处都会挂：
  1. 根 pom 的 `hadoop.version=3.5.0` 抬高 Spark 传递进来的 Hadoop。3.4.1 的 `UserGroupInformation`
     还在调 `Subject.getSubject(...)`，JDK 18+ 抛 `UnsupportedOperationException: getSubject is not supported`，
     而 JDK 25 已不接受 `-Djava.security.manager=allow`，没有别的绕法。
  2. `-Pspark-runner` 排除 `spark-connect-shims_2.13`（经 spark-sql-api 传递进来）：它里面的桩版
     `SparkConf` 会盖住 spark-core 的真实类，报 `NoSuchMethodError: SparkConf.contains`。
  3. Spark 自身是 `provided` 的（生产由 spark-submit 提供），本地直接 `java -cp` 跑要叠加 `-Pspark-local`。
- 两个 runner 都不需要 `--add-opens`。

## 扩展新连接器（约定）
- 实现 `me.jayer.hdata.core.spi.TransformProvider`，一个 provider 只负责一个 `type`（读写各一个）。
  用 `TypedTransformProvider<C>` 可以直接把配置绑定到 Kotlin data class。
- 返回 `RowSource` / `RowTransform` / `RowSink` 三个便捷基类之一，`RowSink.write` 返回死信流（不支持则返 null）。
- 必须在 `src/main/resources/META-INF/services/me.jayer.hdata.core.spi.TransformProvider` 注册。
- 配置键用 `snake_case`（mapper 装了 SNAKE_CASE 命名策略），并且 **`FAIL_ON_UNKNOWN_PROPERTIES` 是开着的**——
  加字段要同步改配置类，否则用户写了会报错。
- 连接器**不要**直接碰 `YAMLMapper`，只用 `TransformConfig`；`error_handling` 由框架摘走，
  通过 `TransformConfig.errorHandling` 传进来。
- **配置项要么真的生效，要么就别收**。"声明了、`validate` 也放行了、实现里却从没读过"是这个仓库
  反复出现的一类 bug，而且全都不会让作业失败，只会让用户以为自己配的东西起了作用：
  Kafka 的 `key_format` / `value_format`、Redis 的 `start_id` / `end_id`、Neo4j 的 `batch_size`、
  Iceberg 的 `write_mode` / `catalog_name` 都这么坏过。加配置项时顺手写一条断言它**确实改变了行为**
  的测试——只断言 `validate` 不抛是拦不住的。暂时做不了的能力要像 `KafkaWriteConfig`
  拒绝 `exactly-once` 那样**显式报错**，别默默退化。
- 死信记录一律带**原始行自己的时间戳与窗口**（`ValueInSingleWindow`）。
  现编 `Instant.now()` + `GlobalWindow` 的话既没法重放，在窗口化的 pipeline 里
  `context.output` 还会直接抛异常；错误信息要用真实的异常，不要拿行里的字段凑一个。

## 测试
`mvn test` 跑全部（约 510 个），**不需要任何外部服务**。端到端测试的替身方案：

| 模块 | 端到端手段 |
|---|---|
| JDBC | H2 内存库 |
| Hive | 本地临时目录当仓库 + `InMemoryHiveMetastore`（`memory://`），八种格式**真的写文件再真的读回来** |
| Kafka | `KafkaIO.withConsumerFactoryFn` 注入 Kafka 自带的 `MockConsumer`，**真的跑 Beam 的 SDF**；写端用 `MockProducer` |
| FTP | Apache FtpServer 起进程内服务，覆盖真实的 `REST` / `STOR` / `APPE` / `RNFR-RNTO` |
| Filesystem | 本地临时目录 |
| Redis | embedded-redis 起进程内真实 Redis，跑完整链路 |
| Iceberg | 本地临时目录当 warehouse + HadoopCatalog，**真的写文件再真的读回来** |
| Debezium | Debezium 自带、不需要数据库的 `SimpleSourceConnector`，**真的启动嵌入式引擎** |
| Neo4j | 没有轻量的进程内替身，用 Mockito 伪造 `Driver` / `Session` / `Transaction`，覆盖行映射、参数绑定、攒批与逐条回退 |
| HBase / MongoDB / Elasticsearch | 没有轻量的进程内替身，只覆盖到编解码、切分、配置校验这些纯逻辑层 |

写连接器测试时至少要有一条 `SerializableUtils.ensureSerializable(...)`：
DoFn 捕获了不可序列化的对象只会在**提交作业时**炸，只调 `processElement` 的单测永远发现不了。
ES 8 的 `Query` / `SortOptions` 就是这么混进去的。

直接调 `@ProcessElement` / `@SplitRestriction` 做单测时用
`me.jayer.hdata.core.testing.CollectingOutputReceiver`——Beam 的 `OutputReceiver` 不是 SAM 接口，
自己写桩要上百行。

`hdata-core`：
- `spec/PipelineSpecLoaderTest`、`spec/WindowingSpecTest`：解析、格式校验、变量替换、窗口声明。
- `graph/PipelineGraphBuilderTest`：chain/composite/死信/嵌套的端到端行为。
- `graph/PipelineWiringTest`：端口校验、复合节点输出、窗口下发。
- `transforms/BuiltinTransformsTest`：每个内置 transform 的正常与错误路径。
- `util/RowConvertersTest`、`error/ErrorSchemasTest`、`spi/TransformConfigTest`、`registry/TransformRegistryTest`。
- `spec/ExamplesTest`：`examples/` 下所有示例必须能解析，改示例要跑这个。
- 测试用的连接器在 `src/test/kotlin/.../testing/TestSinkProvider.kt`，通过测试 resources 的 services 文件注册。

`hdata-jdbc`：
- `JdbcPipelineTest` 用 **H2 内存库**跑真 SQL，覆盖按表读 / query 读 / 分区并行读 / 分表区间 /
  批量写 / 死信。夹具是 `H2Database`，注意它的 URL 带 `DB_CLOSE_DELAY=-1`：
  JdbcSource 在构图阶段会开一次连接推断 schema 再关掉，没有这个参数内存库当场就没了。
- 纯逻辑测试：`internal/SqlTest`、`internal/JdbcMetadataTest`、`internal/TypeMappingsTest`、`partition/*Test`、`JdbcConfigTest`。
- 断言行为时优先把期望写进 pipeline 文件的 `AssertEqual`，写库的结果再用 `H2Database.queryColumn` 核对。
- **H2 会把未加引号的标识符转成大写**，写测试时列名要用 `ID`/`NAME` 而不是 `id`/`name`。
- 验证"分区读是否真的生效"要看 PCollection 的全名（`assertReadStrategy`）：主键探测一旦失败
  会**悄悄**退化成单分区读，只断言行数是发现不了的。
- 各库对同一 SQL 类型上报的 `columnClassName` 不一致（H2 的 SMALLINT 报 `Integer`、CLOB 报
  `java.sql.Clob`，MySQL 的 CLOB 报 `String`），断言别写死 Java 类型。

`hdata-hive`：
- `HivePipelineTest` 用**本地临时目录 + 进程内 metastore**（夹具 `TestHive`）跑端到端：
  八种存储格式各写一遍再读回来、分区目录、分区注册、分区裁剪、列投影、字节区间切分。
- `HiveTypeRoundTripTest` 逐类型验证往返，包括**全 null 的行**——
  空值最容易在某个格式上悄悄变成 `false` / `""`（CSV 的空字段、RCBINARY 的 0 长度单元格都踩过）。
- `RcFileTest` 里有一个**真正由 hive-exec 4.0.1 写出来**的 RCFile 夹具
  （`src/test/resources/hive-written.rc`），用来钉住自己实现的容器格式确实与 Hive 兼容。
  反方向也验证过：Hive 的 `RCFile.Reader` 能完整读出本实现写的文件，两边的输出
  除了那 16 字节随机同步标记之外逐字节相同。夹具是提前生成好放进仓库的，测试不依赖 hive-exec。
- `HiveWriteModeTest` 专测**写第二次**才暴露的问题：追加不能盖掉上一次、覆盖只清本次写到的分区、
  动态分区注册新分区时老分区不能报错、两次作业的文件名不能撞。
- `HiveSerializationTest` 覆盖全部 DoFn、`FileIO.Sink` 与元数据模型的序列化。
- 纯逻辑测试：`type/HiveTypesTest`（嵌套类型解析）、`metastore/PartitionNamesTest`（转义规则）、
  `format/HiveFormatsTest`（格式判定 / 可切分性 / 文本编解码）、`HiveConfigTest`（配置绑定与死信）。

其余模块全部有测试，覆盖到哪一层看上面那张替身表：`hdata-ftp` / `hdata-filesystem` / `hdata-redis` /
`hdata-iceberg` / `hdata-debezium` 有真正的端到端；`hdata-mongodb` / `hdata-hbase` /
`hdata-elasticsearch-6` / `hdata-elasticsearch-8` 只到纯逻辑层，给它们补端到端时
可以参考 `hdata-kafka` 的 Splittable DoFn 与 `hdata-jdbc` 的 H2 写法。

几条**只有写对了测试才守得住**的不变量，都是排查出来的真实 bug，改动相关代码时别把它们弄丢：
- `hdata-ftp` 的 `切分点正好落在行首时，那一行不能丢`：切分点必须取**行长的整数倍**，
  才能覆盖到"读取端一律用 Splittable DoFn"那节的第 4 条铁律。
  原来的用例取 `size/3`，永远落在行中间，这个 bug 就一直没暴露。
- `hdata-iceberg` 的 `BYTES 列往返不丢`：Iceberg 的 binary 要 `ByteBuffer`、Beam 的 BYTES 要 `ByteArray`，
  两个方向的换算曾经写反，而没有任何用例碰过 BYTES 列。
- `hdata-iceberg` 的 `write_mode overwrite 会先清空表`：走完整 provider 链路，
  这样清表那步（side input）才在覆盖范围内。只调 DoFn 是测不到的。
- `hdata-neo4j` 的攒批用例：断言看的是"走没走事务这条路"（`txRuns` / `sessionRuns`），
  不是提交了几次事务——DirectRunner 怎么切 bundle 会直接影响提交次数。
  另外伪造的 `DriverFactory` 必须是 `object` 且计数器线程安全：它要跟 DoFn 一起序列化，
  断言看的又是进程内共享的那份静态状态，而 DirectRunner 会把 bundle 分到多个线程上跑。

行为断言跑在 DirectRunner 上（`AssertEqual` 依赖 runner 执行断言）。

## 其他
- 包名统一为 `me.jayer.hdata.*`。
- 日志用 log4j2，默认配置在 `hdata-core/src/main/resources/log4j2.xml`，可用 `-Dlog4j2.configurationFile` 覆盖。
