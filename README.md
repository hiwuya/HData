## HData

基于 [Apache Beam](https://beam.apache.org/) 的数据同步 / ETL 工具，Kotlin 编写。
作业用一份 pipeline 文件描述，格式对齐 [Beam YAML](https://beam.apache.org/documentation/sdks/yaml/)。

### 快速开始

```bash
mvn -q package

java -cp 'hdata-core/target/classes:hdata-jdbc/target/classes:<依赖>' \
  me.jayer.hdata.core.HData --pipeline=examples/jdbc-to-jdbc.yaml
```

常用参数：

| 参数 | 说明 |
|---|---|
| `--pipeline=<path>` | pipeline 文件，支持 `.yaml` / `.yml` / `.toml` / `.json` |
| `--dryRun` | 只构图并打印 DAG，不提交运行 |
| `--runner=DirectRunner` | 任何 Beam `PipelineOptions` 都可以从命令行传 |
| `--waitUntilFinish=false` | 提交后不等待，适合流式作业 |

Runner 默认只带 DirectRunner，跑 Flink / Spark 用 `mvn -Pflink-runner package` / `-Pspark-runner`。

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
- `jdbc-to-jdbc.toml` —— 同一个作业的 TOML 写法（兼容保留，不推荐）
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

### 扩展新连接器

1. 实现 `TransformProvider`（或 `TypedTransformProvider<C>`），返回
   `RowSource` / `RowTransform` / `RowSink`；
2. 在 `META-INF/services/me.jayer.hdata.core.spi.TransformProvider` 注册；
3. 新模块加进根 pom 的 `<modules>` 并依赖 `hdata-core`。

### 设计说明

架构设计、TOML/YAML 选型分析、与 Beam 编程指南的对照见 [ARCHITECTURE.md](ARCHITECTURE.md)。
