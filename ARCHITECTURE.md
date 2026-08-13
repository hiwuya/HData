# HData 架构设计

对照 [Beam 编程指南](https://beam.apache.org/documentation/programming-guide/) 与
[Beam YAML 规范](https://beam.apache.org/documentation/sdks/yaml/) 重新设计后的架构说明。

---

## 一、重构前的问题

### 1.1 图模型只能表达"一条直线"，而且连直线都表达错了

重构前的 `HData.start()`：

```kotlin
for (source in sources) {
    var input = pipeline.apply(source)
    transforms.forEach { input = input.apply(it) }
    sinks.forEach { input.apply(it) }
}
```

配置模型是 `sources / transforms / sinks` 三个平铺数组，于是：

- **N 个 source × M 个 sink 会被全连接**：两个 source 两个 sink，得到的是 4 条写入路径，
  而不是用户想要的两条。作业越大越容易写出意料之外的图。
- **分支、合流、join 一概表达不了**。Beam 的核心模型是"PCollection 当变量、PTransform 当函数"，
  可以拼出任意 DAG；三段式配置把这个能力砍掉了。
- **transform 无法只作用于某一路**，所有 transform 强制作用于所有 source。

### 1.2 SPI 把 source 和 sink 焊死在一个类里

```kotlin
interface StructuredIOProvider {
    fun identifier(): String
    fun createSource(config: Map<String, Any>): StructuredSource
    fun createSink(config: Map<String, Any>): StructuredSink
}
```

一个只读的连接器（比如某个 HTTP 数据源）必须实现一个抛异常的 `createSink`。
读写两端共用一个 `identifier`，配置也就只能挤在同一个命名空间里。

### 1.3 连接器知道了配置文件的格式

```kotlin
// JdbcStructuredIOProvider
val descriptor = ObjectMappers.getTomlObjectMapper().convertValue(config, JdbcSourceDescriptor::class.java)
```

连接器直接依赖 `TomlMapper`。想换格式，得改每一个连接器。

### 1.4 配置错了不报错

`FAIL_ON_UNKNOWN_PROPERTIES` 是关掉的：`partitionNum` 写成 `partionNum` 会被静默丢弃，
作业照跑，只是不分区了——这类问题在生产上很难查。

### 1.5 没有错误处理通道

一条脏数据写不进目标库，整个作业挂掉。Beam 编程指南里的 dead-letter 模式没有落地。

### 1.6 其他

- 包重命名遗留了大量全限定名（`me.jayer.hdata.jdbc.util.JdbcUtils.createDataSource(...)`），可读性很差。
- `beam-runners-flink` 排除了全部 `org.apache.flink:*`，导致 `PipelineOptionsFactory`
  在扫描 `PipelineOptionsRegistrar` 时因 `FlinkPipelineOptions` 引用不到类而**静态初始化失败**，
  程序根本起不来。
- `JdbcTypeRegistry` 处理 ARRAY 列的分支里残留 `TODO("Not yet implemented")`，返回的是一个
  handler 对象而不是数组值。
- `SqlRunner` 的 vararg 转发漏了展开符（`query(it, sql, handler, args)`），带参查询会把整个数组当成一个参数。

---

## 二、配置格式选型：TOML vs YAML

### 2.1 结论

**pipeline 文件只支持 YAML，并且直接采用 Beam YAML 的方言。**

原来的 TOML 格式不再支持——多格式并存的收益（几乎为零，反正都要解析成同一棵语法树）
抵不过它的代价：两份等价示例要同步维护、两套写法要同时出现在文档里、
用户还要先纠结用哪个。解析结果依然是格式无关的 `JsonNode`，
连接器不感知文件长什么样，将来真要加格式也只是多注册一个 mapper 的事。

### 2.2 为什么不是 TOML

TOML 的设计目标是"人类可读的最小配置文件格式"，它在**扁平的键值配置**上非常好用
（`Cargo.toml`、`pyproject.toml` 都是这个形态）。但 pipeline 文件描述的是一张 **DAG**，
这正好是 TOML 最弱的场景：

| 维度 | TOML | YAML |
|---|---|---|
| 嵌套结构 | 靠表头全路径表达，每深一层就多一截前缀 | 缩进天然表达层级 |
| 数组套对象套数组 | `[[pipeline.transforms.transforms]]`，且顺序敏感 | 缩进即可 |
| 视觉分组 | 数组元素之间没有边界，节点多了难分辨 | `- ` 一眼看清节点边界 |
| 多行字符串（SQL） | 支持 `"""`，但要注意转义 | `|` 块字面量，SQL 原样贴进来 |
| 复用 | 无 | 锚点 `&`/`*`（本项目暂未启用） |
| 生态 | 主要是语言包管理器 | K8s / Argo / Airflow / CI 全都是 YAML |
| 与 Beam 官方的关系 | 无 | **Beam YAML 就是官方规范**，官方文档的例子可以直接抄 |

最直观的例子是 `examples/branching.yaml` 里的嵌套 chain：写成 TOML 就是
`[[pipeline.transforms.transforms]]`，再深一层基本没法读；YAML 里只是多一层缩进。

TOML 的优势（无歧义、没有缩进敏感、没有 YAML 1.1 的 "Norway problem"）在这里份量不够：
pipeline 文件是**结构主导**的，不是**取值主导**的。

### 2.3 为什么跟着 Beam YAML 的方言走

Beam 官方的 YAML SDK 是 Python 实现的，本项目在 Kotlin 上自己实现了一遍等价的语义。
沿用同一套方言（`pipeline` / `type: chain` / `transforms` / `input` / `config` /
`error_handling` / `extra_transforms` / `windowing`）的好处：

1. 用户的知识可以迁移，官方文档的示例结构直接可用；
2. transform 命名跟着官方走（`ReadFromJdbc` / `WriteToJdbc` / `MapToFields` / `Flatten` /
   `LogForTesting` / `AssertEqual` / `StripErrorMetadata`）；
3. 配置键用 `snake_case`，与官方一致；
4. 将来若要与官方 YAML SDK 互通（同一份文件两边都能跑），差距最小。

**与官方的差异**（本项目暂不支持的部分）：`language: python/javascript` 的 UDF 表达式、
`Sql` transform、`providers`（跨语言扩展服务）、`error_handling.threshold`。
其中 `threshold` 是**显式报错**而不是静默忽略。

---

## 三、重构后的架构

### 3.1 分层

```
hdata-core
├── spec/        pipeline 文件的语法树 + 解析 + 变量替换 + 结构校验（不依赖 Beam 语义）
├── spi/         连接器扩展点：HDataTransform / TransformProvider / TransformConfig
├── registry/    type -> provider 的注册表（HData SPI + Beam 原生 SchemaTransformProvider 桥接）
├── graph/       语法树 -> Beam DAG 的构建器（chain / composite / 引用解析 / 死信 / 窗口）
├── transforms/  内置 transform
├── error/       死信记录的 schema 约定
├── util/        JsonNode <-> Beam Row 转换
└── HData.kt     入口：解析参数 -> 加载文件 -> 构图 -> 提交
```

数据流是单向的：`spec` 不认识 Beam，`graph` 不认识文件格式，`transforms`/连接器不认识两者。

### 3.2 统一的 transform 契约：直接用 Beam 的 `SchemaTransform`

```kotlin
abstract class HDataTransform : SchemaTransform()
// 即 PTransform<PCollectionRowTuple, PCollectionRowTuple>
```

这是本次重构最关键的一步。原来的 `StructuredSource` / `StructuredTransform` / `StructuredSink`
是三个签名互不相同的类型（`PBegin->PCollection`、`PCollection->PCollection`、`PCollection->PDone`），
构图器必须对三种类型分别处理，也就顺理成章地写成了"source 段 / transform 段 / sink 段"。

改成 `PCollectionRowTuple -> PCollectionRowTuple` 之后：

- **只有一种节点**，输入输出的**基数**（0/1/N）区分读取端、处理端、写入端，构图器只处理一种契约；
- 每个节点天然支持**多输入**（join、flatten）和**多输出**（主输出 + 死信）；
- HData 的连接器**就是合法的 Beam `SchemaTransform`**，可以被原生 Beam Java pipeline 直接复用；
- 反过来，classpath 上任何 Beam 原生 `SchemaTransformProvider`（例如
  `beam:schematransform:org.apache.beam:jdbc_read:v1`）都能通过
  `BeamSchemaTransformAdapter` 直接写进 pipeline 文件，不用为每个都写一遍连接器。

日常写连接器不必直接面对 tuple，用三个便捷基类即可：

```kotlin
abstract class RowSource   : HDataTransform() { protected abstract fun read(begin: PBegin): PCollection<Row> }
abstract class RowTransform: HDataTransform() { protected abstract fun transform(input: PCollection<Row>): PCollection<Row> }
abstract class RowSink     : HDataTransform() { protected abstract fun write(input: PCollection<Row>): PCollection<Row>? /* 死信 */ }
```

### 3.3 一个 provider 只管一个 type

```kotlin
interface TransformProvider {
    fun identifier(): String                       // pipeline 文件里的 type
    fun inputCollectionNames(): List<String>       // 空 = 读取端；["*"] = 变元（Flatten）
    fun outputCollectionNames(): List<String>      // 空 = 写入端
    fun from(config: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple>
}
```

`ReadFromJdbc` 与 `WriteToJdbc` 是两个独立 provider，读写两端各自演进配置，
只读连接器也不用再写空实现。端口声明是元数据，构图器据此校验用户写的 `input`。

### 3.4 配置绑定：连接器不感知文件格式

`TransformConfig` 只暴露格式无关的语法树，绑定到 Kotlin data class：

```kotlin
class JdbcReadProvider : TypedTransformProvider<JdbcReadConfig>(JdbcReadConfig::class.java) {
    override fun create(config: JdbcReadConfig, context: TransformConfig) = ...
}
```

配套改动：

- `snake_case` 命名策略：文件里写 `fetch_size`，Kotlin 侧还是 `fetchSize`；
- **开启 `FAIL_ON_UNKNOWN_PROPERTIES`**：拼错的键直接报错，不再静默丢弃；
- `error_handling` 由框架先摘走（通过 `context.errorHandling` 传给需要的连接器），
  所以配置类可以安全地开严格模式。

### 3.5 构图：引用式 DAG

`PipelineGraphBuilder` 把语法树翻译成 Beam DAG：

- **chain**：线性，输入由上一个节点隐式提供，不允许写 `input`（写了直接报错）；
- **composite**：任意 DAG，`input: 名字` / `input: {A: x, B: y}` / `input: [x, y]`；
  按依赖**拓扑排序**后构建，所以书写顺序无关；成环会被检测并打印环路；
- **引用语法** `名字.输出端口`，死信流就是通过 `名字.<error_handling.output>` 被消费的；
- **嵌套**：`type: chain` / `type: composite` 可以作为子节点出现，每层是独立命名空间，
  内层可以向外层查名；子节点用 `input` 这个保留名引用复合节点自身的输入；
- **`source` / `sink` 简写**与 **`extra_transforms`**（在 chain 之外挂节点，典型用途是消费错误流）；
- **窗口**：节点级 `windowing` 作用于它的输入，pipeline 级作用于根节点的输出。

节点名同时用作 Beam 的 transform 名（嵌套时带上作用域路径），DAG 在 runner UI 上可读。

### 3.6 死信

`error_handling` 是 config 里的一个块，与 Beam YAML 一致：

```yaml
- type: WriteToJdbc
  name: WriteOrders
  config:
    table: dws_order
    error_handling:
      output: rejected
```

框架的约束：**声明了就必须被消费**，否则构图阶段直接报错。
这条规则来自 Beam YAML，作用是杜绝"以为兜住了其实丢了"。

死信记录保留原始行而不是把它序列化成字符串，因此错误流依旧是带 schema 的 `PCollection<Row>`：

```
element       ROW<原始 schema>  NULLABLE
error_type    STRING
error_message STRING            NULLABLE
transform     STRING
```

`StripErrorMetadata` 可以还原出原始记录，修数后重放。

JDBC 写入端的实现策略：批量写失败时，如果开了死信就**退回逐条写**定位坏数据，
只有真正写不进去的那几条进死信流；没开死信则维持原行为直接抛异常。

### 3.7 入口与运行

`HData.main` 只剩四步：解析命令行 → 加载 pipeline 文件 → 构图 → 提交。
文件里的 `options:` 作为 Beam `PipelineOptions` 的默认值，命令行参数优先级更高。

新增：

- `--dryRun`：只构图并打印 DAG，不提交；
- `${VAR}` / `${VAR:-默认值}` 变量替换（显式变量 > 系统属性 > 环境变量），把密码挪出配置文件。
  变量名必须匹配 `[A-Za-z_][A-Za-z0-9_.]*`，因此不会误伤 JDBC 的表名区间语法 `${00-15}`；
- runner 改为 Maven profile（`-Pflink-runner` / `-Pspark-runner`），默认只带 DirectRunner，
  修复了 1.6 里那个"程序根本起不来"的问题。

---

## 四、对照 Beam 编程指南的落地情况

| Beam 概念 | 落地位置 |
|---|---|
| Pipeline / PipelineOptions | `HData.build()`，`HDataOptions` + 文件里的 `options:` |
| PCollection / Schema / Row | 全链路统一走 `PCollection<Row>`，schema 由连接器推断 |
| PTransform | `HDataTransform`（= Beam `SchemaTransform`） |
| 复合 transform | `type: chain` / `type: composite`，可嵌套 |
| Coder | 依赖 schema 自动推导（`setRowSchema`），未自定义 |
| DoFn 生命周期 | JDBC 读写端使用 `@Setup`/`@StartBundle`/`@ProcessElement`/`@FinishBundle`/`@Teardown` |
| Splittable DoFn | `JdbcSourceSplittableDoFn`，按分区列切 `OffsetRange` |
| 多输出 / TupleTag | 由 `PCollectionRowTuple` 的命名端口承担 |
| dead-letter 模式 | `error_handling` + `ErrorSchemas` + `StripErrorMetadata` |
| 窗口 | `windowing:`（fixed / sliding / sessions / global） |
| Metrics | JDBC 写入端的批大小、批耗时、失败条数 |

**尚未覆盖**：side input、GroupByKey/Combine 类聚合、触发器与 accumulation mode、
state & timer、跨语言 transform（`providers`）。这些在"数据同步"这个主场景里用不到，
但 `SchemaTransform` 契约本身不构成阻碍，需要时按 transform 增量补即可。

---

## 五、写一个新连接器要做什么

1. 实现 `TransformProvider`（或 `TypedTransformProvider<C>`），返回一个
   `RowSource` / `RowTransform` / `RowSink`；
2. 在 `META-INF/services/me.jayer.hdata.core.spi.TransformProvider` 里注册；
3. 新模块加进根 pom 的 `<modules>` 并依赖 `hdata-core`。

参考 `hdata-jdbc`（完整实现）或 `hdata-core` 测试目录里的 `TestSinkProvider`（最小实现，含死信）。
