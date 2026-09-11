[![Build](https://github.com/stuxuhai/HData/actions/workflows/ci.yml/badge.svg)](https://github.com/stuxuhai/HData/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![JDK](https://img.shields.io/badge/JDK-25-orange.svg)](https://openjdk.org/)
[![Maven](https://img.shields.io/badge/build-Maven%204-0A0.svg)](https://maven.apache.org/)

## HData

基于 [Apache Beam](https://beam.apache.org/) 的数据同步 / ETL 工具，Kotlin 编写。
作业用一份 pipeline 文件描述，格式对齐 [Beam YAML](https://beam.apache.org/documentation/sdks/yaml/)。

> English documentation: [README.md](README.md).

### 特性

- **YAML 描述作业**：source / transform / sink 全用一份 pipeline 文件声明，无需写代码即可完成常见同步。
- **对齐 Beam YAML 规范**：配置结构、变量替换、`${VAR}` 占位与 Beam 官方 YAML 一致，可平滑迁移。
- **丰富连接器**：JDBC、Kafka、Hive、Redis、Neo4j、Iceberg、Debezium(CDC)、MongoDB、HBase、FTP、Filesystem、Elasticsearch 6/8。
- **死信（Dead Letter）**：坏数据单独落下游而不是让作业崩溃，且死信带原始行的时间戳与窗口，可重放。
- **多 Runner**：同一份 pipeline 可在 DirectRunner / FlinkRunner / SparkRunner 上运行。
- **聚合下推**：部分连接器把 sum / avg / count 等聚合下推到数据源原生执行。
- **有向无环图构图**：支持 chain / composite、分支合流、嵌套、拓扑排序与窗口下发。

### 支持的连接器

| 连接器 | 读取 | 写入 | 测试覆盖 |
|---|:---:|:---:|---|
| JDBC | ✅ | ✅ | 端到端（H2 内存库） |
| Kafka | ✅ | ✅ | 端到端（MockConsumer，真跑 SDF） |
| Hive | ✅ | ✅ | 端到端（本地目录 + 进程内 metastore） |
| Redis | ✅ | ✅ | 端到端（embedded-redis） |
| Iceberg | ✅ | ✅ | 端到端（本地 warehouse + HadoopCatalog） |
| Debezium | ✅ (CDC) | — | 端到端（嵌入式引擎） |
| FTP | ✅ | ✅ | 端到端（进程内 FtpServer） |
| Filesystem | ✅ | ✅ | 端到端（本地临时目录） |
| Neo4j | ✅ | ✅ | 逻辑层（Mockito） |
| MongoDB | ✅ | ✅ | 逻辑层 |
| HBase | ✅ | ✅ | 逻辑层 |
| Elasticsearch 6 | ✅ | ✅ | 逻辑层 |
| Elasticsearch 8 | ✅ | ✅ | 逻辑层 |

所有连接器的 `Read*` / `Write*` 配置参数（类型、默认值、约束与互斥关系）见 [docs/connectors.md](docs/connectors.md)。

### 快速开始

```bash
mvn -q package

java -cp 'hdata-core/target/classes:hdata-jdbc/target/classes:<依赖>' \
  me.jayer.hdata.core.HData --pipeline=examples/jdbc-to-jdbc.yaml
```

常用参数：

| 参数 | 说明 |
|---|---|
| `--pipeline=<path>` | pipeline 文件（`.yaml` / `.yml`） |
| `--dryRun` | 只构图并打印 DAG，不提交运行 |
| `--runner=DirectRunner` | 任何 Beam `PipelineOptions` 都可以从命令行传 |
| `--waitUntilFinish=false` | 提交后不等待，适合流式作业 |

Runner 默认只带 DirectRunner：

| profile | runner 依赖 | 说明 |
|---|---|---|
| 无 | `beam-runners-direct-java` | 默认，`--runner=DirectRunner` |
| `-Pflink-runner` | `beam-runners-flink-2.2` | `--runner=FlinkRunner` |
| `-Pspark-runner` | `beam-runners-spark-4` | `--runner=SparkRunner`，Spark 自身由 spark-submit 提供 |
| `-Pspark-local` | Spark 4 本体 | 叠加在 `-Pspark-runner` 上，本地直接 `java -cp` 跑时才需要 |

> Spark 传递进来的 Hadoop 被根 pom 抬到了 **3.5.0**。Spark 4.0.2 自带的 Hadoop 3.4.1 里
> `UserGroupInformation` 还在调用 `Subject.getSubject(...)`，在 JDK 18+ 会抛
> `UnsupportedOperationException: getSubject is not supported`，而 JDK 25 又不再接受
> `-Djava.security.manager=allow`。降级 Hadoop 会让 Spark runner 起不来。

### 从源码构建

要求：

- **JDK 25**（目标字节码 Java 25）
- **Maven 4**（本项目未提供 wrapper）

```bash
# 首次构建需联网拉取依赖（Maven Central）
mvn -q package

# 跑全部测试（约 1900 个，不需要任何外部服务）
mvn -q test
```

程序入口是 `me.jayer.hdata.core.HData`，运行方式见上方「快速开始」。

### pipeline 文件

线性作业用 `chain`，输入由上一个节点隐式提供：

```yaml
pipeline:
  type: chain
  source:
    type: ReadFromJdbc
    config:
      url: "jdbc:mysql://127.0.0.1:3306/demo?useSSL=false"
      user: root
      password: "${MYSQL_PASSWORD}"
      tables: ["t_order"]
      partition_column: id
      partition_num: 8
  transforms:
    - type: MapToFields
      config:
        fields:
          order_id: id
          amount: total_amount
  sink:
    type: WriteToJdbc
    config:
      url: "jdbc:mysql://127.0.0.1:3306/dw?useSSL=false"
      user: root
      password: "${MYSQL_PASSWORD}"
      table: dws_order

options:
  runner: DirectRunner
```

有分支或合流就用 `composite`（省略 `type` 时的默认值），节点靠 `input` 互相引用，
书写顺序不影响构图：

```yaml
pipeline:
  transforms:
    - type: Flatten
      name: AllOrders
      input: [ReadHot, ReadArchived]
    - type: ReadFromJdbc
      name: ReadHot
      config: { ... }
    - type: ReadFromJdbc
      name: ReadArchived
      config: { ... }
```

更多写法见 `examples/`：

- `jdbc-to-jdbc.yaml` —— 单表同步
- `dead-letter.yaml` —— 死信：坏数据单独落表而不是让作业挂掉
- `branching.yaml` —— 多路读入、合流、分支、嵌套 chain

配置里的 `${VAR}` / `${VAR:-默认值}` 会在解析前替换，取值顺序是系统属性 → 环境变量，
用来把密码挪出配置文件。

### 内置 transform

| type | 说明 |
|---|---|
| `Create` | 用字面量造数据，schema 自动推断 |
| `MapToFields` | 字段选择 / 改名 / 丢弃（`append` + `drop`） |
| `Flatten` | 合并多路同 schema 的输入 |
| `LogForTesting` | 打印每条记录并透传 |
| `StripErrorMetadata` | 把死信记录还原成原始记录 |
| `AssertEqual` | 断言输入等于给定集合，用来给 pipeline 文件写测试 |

连接器（`hdata-jdbc`）：`ReadFromJdbc` / `WriteToJdbc`。

classpath 上的 Beam 原生 `SchemaTransformProvider` 也可以直接用它的 URN 当 `type`，
例如 `beam:schematransform:org.apache.beam:jdbc_read:v1`。

### 死信

在 sink 的 config 里声明 `error_handling.output`，错误流就以 `<节点名>.<output>` 暴露出来，
**且必须被下游消费**，否则构图阶段直接报错：

```yaml
    - type: WriteToJdbc
      name: WriteOrders
      config:
        table: dws_order
        error_handling:
          output: rejected
  extra_transforms:
    - type: StripErrorMetadata
      name: Recovered
      input: WriteOrders.rejected
```

### 连接器配置参考

所有连接器（JDBC / Kafka / Hive / Redis / Neo4j / Iceberg / Debezium / MongoDB / HBase / FTP /
Filesystem / Elasticsearch 6/8）的 `Read*` / `Write*` 配置参数（类型、默认值、约束与互斥关系）
见 [docs/connectors.md](docs/connectors.md)。

### 扩展新连接器

1. 实现 `TransformProvider`（或 `TypedTransformProvider<C>`），返回
   `RowSource` / `RowTransform` / `RowSink`；
2. 在 `META-INF/services/me.jayer.hdata.core.spi.TransformProvider` 注册；
3. 新模块加进根 pom 的 `<modules>` 并依赖 `hdata-core`。

### 设计说明

架构设计、配置格式选型分析、与 Beam 编程指南的对照见 [ARCHITECTURE.md](ARCHITECTURE.md)；
聚合下推的设计见 [PUSHDOWN.md](PUSHDOWN.md)。

## 许可证

HData 基于 [Apache License 2.0](LICENSE) 发布。版权归 The HData Authors，并基于
[Apache Beam](https://beam.apache.org/)（同样为 Apache 2.0）构建。

## 参与贡献

欢迎提 Issue 与 Pull Request。开发环境搭建、构建测试、代码约定与新增连接器的步骤见
[CONTRIBUTING.md](CONTRIBUTING.md)。社区行为准则见 [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)。
安全漏洞请按 [SECURITY.md](SECURITY.md) 的方式私下上报，不要公开讨论。

> 注：本仓库的 `AGENTS.md` 是给 AI 编程助手看的工程约定，普通贡献者无需关注。
