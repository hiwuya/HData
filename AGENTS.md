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
  - `internal/`：实现细节，但**已对 `hdata-hive` 开放**——Hive 的读写本质就是 JDBC，
    schema 推断、类型映射、连接池、行绑定全部复用这里，不要再抄一份。
    `TypeMappings` 是**不可变**的规则表，每列只解析一次，解析结果随 DoFn 序列化下发——
    往里加东西时注意**不要捕获普通 Kotlin lambda**，捕获 `Function1` 会让整个 DoFn 无法序列化
    （用 `ValueConverter` 这类可序列化 fun interface）。
  - `partition/`：分区列的选取与校验。`transform/`：三个 DoFn，连接池一律 `@Setup` 建、`@Teardown` 关。
- `hdata-kafka`：`ReadFromKafka` / `WriteToKafka`。读取**直接复用 Beam 的 `ReadFromKafkaDoFn`**，
  本模块只负责 `internal/KafkaOffsets`：把 Flink 风格的 startup/bounded 模式翻译成每分区的起止偏移量。
- `hdata-hive`：`ReadFromHive` / `WriteToHive`，通过 `hive-jdbc` 访问 HiveServer2，实现复用 `hdata-jdbc`。
  `HivePartitions` 负责把 `SHOW PARTITIONS` 的 `dt=2024-01-01/hr=01` 翻译成合法谓词。
- `hdata-mongodb`：`ReadFromMongoDb` / `WriteToMongoDb`。`internal/MongoBuckets` 用 `$bucketAuto` 求 `_id`
  分桶边界，读取按桶下标切分；写入走 `bulkWrite`，支持 `upsert_keys`。
- `hdata-hbase`：`ReadFromHBase` / `WriteToHBase`。扫描**复用 Beam 的 `HBaseIO.readAll()`**
  （注意不是 `HBaseIO.read()`，后者内部还是老的 `BoundedSource`）。`HBaseRowCodec` 管行编解码。
- `hdata-ftp`：`ReadFromFtp` / `WriteToFtp`。按**字节区间**并行读，靠 FTP 的 `REST` 命令定位起点；
  写入是分片的，每个 bundle 先写 `.tmp` 再 `rename`。
- `hdata-filesystem`：`ReadFromFilesystem` / `WriteToFilesystem`。匹配用 `FileIO.match()`，
  text 读取用 `TextIO.readFiles()`（真正的字节区间切分），落盘用 `FileIO.write()`（分片 + 原子改名）。
- `hdata-elasticsearch-6` / `hdata-elasticsearch-8`：按 ES 原生 **slice** 并行读（`scan_slices`）。

配置类只依赖 `TransformConfig.bind(...)`（Jackson 3），不要自己 new `YAMLMapper`；
写路径用 `@Setup`/`@FinishBundle`/`@Teardown` 管资源，失败行经 `ErrorSchemas.failure(...)` 进死信。

## 读取端一律用 Splittable DoFn（重要）

**能复用 Beam 官方 IO 的就不要自己写**：Kafka / HBase / Filesystem 已经换成官方实现（见上面的模块说明）。
其余模块（JDBC / Hive / MongoDB / Elasticsearch / FTP）Beam 没有 SDF 版实现，是自写的。
自己写 SDF 时有三条铁律，都是这轮重构里踩出来的：

1. **不要 `tryClaim(range.to - 1)` 一次性认领整段**。那等于告诉 Beam"这段不可再分"，
   运行时既没法把剩下的活分给空闲 worker，也拿不到进度。正确写法是循环里逐个认领。
2. **`checkDone()` 有契约**：`OffsetRangeTracker` 要求最后一次*尝试*的偏移量 >= `to - 1`。
   提前读完（比如文件到了 EOF）要补一次 `tryClaim(range.to)`——它返回 false 但记下这次尝试，
   否则报 `claiming work in [x, y) was not attempted`。
3. **有些格式天生不能按字节切**：带引号的 CSV 字段可以内嵌换行，从任意字节位置切开会把记录劈成两半；
   xlsx 是 zip 容器只能从头解析。这两种的并行度来自文件个数，代码里要写清楚为什么。

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

## 测试
`mvn test` 跑全部（约 410 个），**不需要任何外部服务**。端到端测试的替身方案：

| 模块 | 端到端手段 |
|---|---|
| JDBC / Hive | H2 内存库（Hive 走的是纯 JDBC，这条链路和 HiveServer2 一样） |
| Kafka | `KafkaIO.withConsumerFactoryFn` 注入 Kafka 自带的 `MockConsumer`，**真的跑 Beam 的 SDF**；写端用 `MockProducer` |
| FTP | Apache FtpServer 起进程内服务，覆盖真实的 `REST` / `STOR` / `APPE` / `RNFR-RNTO` |
| Filesystem | 本地临时目录 |
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

`hdata-kafka` 之前只有 pom 占位，现已实现读写源码；`hdata-hive`/`hdata-mongodb`/`hdata-hbase`/`hdata-ftp`/
`hdata-filesystem`/`hdata-elasticsearch-6`/`hdata-elasticsearch-8` 目前只有主源码、尚未补测试（测试桩可参考
`hdata-kafka` 的 Splittable DoFn 与 `hdata-jdbc` 的 H2 端到端写法）。

行为断言跑在 DirectRunner 上（`AssertEqual` 依赖 runner 执行断言）。

## 其他
- 包名统一为 `me.jayer.hdata.*`。
- 日志用 log4j2，默认配置在 `hdata-core/src/main/resources/log4j2.xml`，可用 `-Dlog4j2.configurationFile` 覆盖。
