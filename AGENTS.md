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
- `hdata-jdbc`：JDBC 连接器，两个 provider：`ReadFromJdbc` / `WriteToJdbc`。
  - `internal/`：不对外的实现细节。`TypeMappings` 是**不可变**的规则表，每列只解析一次，
    解析结果随 DoFn 序列化下发——往里加东西时注意**不要捕获普通 Kotlin lambda**，
    捕获 `Function1` 会让整个 DoFn 无法序列化（用 `ValueConverter` 这类可序列化 fun interface）。
  - `partition/`：分区列的选取与校验。
  - `transform/`：三个 DoFn，连接池一律 `@Setup` 建、`@Teardown` 关。
- `hdata-kafka`：Kafka 连接器（`kafka-clients` 4.3.1），两个 provider：`ReadFromKafka` / `WriteToKafka`。
  - `transform/KafkaReadFn.kt`：参考实现的 **Splittable DoFn** 模板——`DoFn<ConsumerRecord<*,*>, Row>` 配 `OffsetRange` +
    `OffsetRangeTracker` 做分区内 seek 偏移拆分。`transform/KafkaWriteFn.kt` 走 `@FinishBundle` 批量发送，失败进死信。
  - 配置字段对齐 Flink Kafka SQL connector（`ReadFromKafka`：`topic`/`topics`/`topic_pattern`、`bootstrap_servers`、
    `consumer_group`、`format`/`value_format`（`json`/`avro`/`csv`）、`auto_offset_reset` 等；
    `WriteToKafka`：`topic`、`bootstrap_servers`、`format`、`batch_size`、`linger_ms`、`acks` 等）。
- `hdata-hive`：Hive 连接器（`hive-jdbc` 4.0.1），`ReadFromHive` / `WriteToHive`，复用 JDBC 通道访问 HiveServer2。
- `hdata-mongodb`：MongoDB 连接器（`mongodb-driver-sync` 5.4.0），`ReadFromMongoDb` / `WriteToMongoDb`，读走 Splittable DoFn 按查询分片拆分。
- `hdata-hbase`：HBase 连接器（`hbase-client` / `hbase-common` 2.6.1），`ReadFromHBase` / `WriteToHBase`，读基于 Region 范围做 Splittable 拆分。
- `hdata-ftp`：FTP/SFTP 连接器（`commons-net` 3.11.1），`ReadFromFtp` / `WriteToFtp`，按文件列表做 Splittable 拆分。
- `hdata-filesystem`：文件系统连接器（Hadoop `hadoop-common` 3.5.0），`ReadFromFilesystem` / `WriteToFilesystem`，
  读用 Beam `FileIO`/`TextIO` 风格的 Splittable 文件拆分。
- `hdata-elasticsearch-6`：Elasticsearch 6.x 连接器（`elasticsearch` 6.8.23 + `elasticsearch-rest-high-level-client` 6.8.23），`ReadFromElasticsearch6` / `WriteToElasticsearch6`。
- `hdata-elasticsearch-8`：Elasticsearch 8.x 连接器（`elasticsearch-java` 8.17.0 + `elasticsearch-rest-client` 8.17.0），`ReadFromElasticsearch8` / `WriteToElasticsearch8`。

  每个连接器模块的读路径统一实现为 **Splittable DoFn**（参考 `hdata-kafka/.../transform/KafkaReadFn.kt`）：
  `@DoFn.BoundedPerElement` + `@GetInitialRestriction` / `@SplitRestriction` / `@NewTracker` / `@GetRestrictionCoder`
  （用 `OffsetRange` + `OffsetRangeTracker`），`@ProcessElement` 产出 `Row`。写路径用 `@Setup`/`@FinishBundle`/`@Teardown`
  管理资源，失败行经 `ErrorSchemas.failure(...)` 进死信。配置类只依赖 `TransformConfig.bind(...)`（Jackson 3），
  不要自己 new `YAMLMapper`。

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
`mvn test` 跑全部（约 148 个），不需要任何外部服务。

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
- 纯逻辑测试：`statement/StatementTest`、`util/TableRangeTest`、`partition/PartitionConvertersTest`、`JdbcConfigTest`。
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
