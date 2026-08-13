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
- `hdata-kafka`：**仅 pom.xml，没有任何源码**（占位模块），别在这里找 Kafka 实现。

## 运行
- `me.jayer.hdata.core.HData --pipeline=<文件>`，另有 `--dryRun`（只构图打印）、`--waitUntilFinish`。
- pipeline 文件顶层是 `pipeline:` + 可选的 `options:`；`pipeline` 本身就是一个 composite/chain 形态的 transform 节点。
- 样例见 `examples/`；设计说明见 `ARCHITECTURE.md`。
- **Runner 是 Maven profile**：默认只有 DirectRunner，Flink/Spark 用 `-Pflink-runner` / `-Pspark-runner`。
  历史上 runner 依赖排除了引擎自身的 jar，会让 `PipelineOptionsFactory` 静态初始化直接失败，别再那么写。

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
- `mvn test`，全部在 `hdata-core`：
  - `spec/PipelineSpecLoaderTest`：解析、格式校验、变量替换、错误信息。
  - `graph/PipelineGraphBuilderTest`：端到端跑在 DirectRunner 上，断言写在 pipeline 文件里（`AssertEqual`）。
  - `spec/ExamplesTest`：`examples/` 下所有示例必须能解析，改示例要跑这个。
- 测试用的连接器在 `src/test/kotlin/.../testing/TestSinkProvider.kt`，通过测试 resources 的 services 文件注册。
- 没有需要真实数据库的测试；JDBC 连接器的 schema 推断发生在**构图阶段**（要连库），所以 `--dryRun` 对 JDBC 作业也需要能连上库。

## 其他
- 包名统一为 `me.jayer.hdata.*`。
- 日志用 log4j2，默认配置在 `hdata-core/src/main/resources/log4j2.xml`，可用 `-Dlog4j2.configurationFile` 覆盖。
