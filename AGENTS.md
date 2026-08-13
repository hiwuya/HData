# AGENTS.md

HData —— 基于 Apache Beam 的数据同步/ETL 工具，Kotlin + Java 混合编写，作业配置用 TOML。

## 构建环境
- 使用 **JDK 25** 构建（目标字节码 Java 25，Kotlin `jvmTarget` 已设为 25）。Kotlin 需 **>= 2.x** 才能在 JDK 25 上运行，当前为 2.4.10；降到 1.8.x 会崩溃（`IllegalArgumentException: 25.0.3`）。
- 使用 Maven 4（`mvn` 命令，**无 maven wrapper**）；依赖走 `~/.m2/settings.xml` 里的 aliyun 镜像，首次构建需联网。
- 根 pom 未标 `<root="true">`，Maven 4 会打印 "Unable to find the root directory" 警告，可忽略。

## 模块结构
- `hdata-core`：核心引擎与程序入口。`me.jayer.hdata.core.HData` 的 `main` 是唯一入口。
- `hdata-jdbc`：JDBC 连接器（`identifier = "jdbc"`），含 source / sink 实现。
- `hdata-kafka`：**仅 pom.xml，没有任何源码**（占位模块），别在这里找 Kafka 实现。

## 运行
- 入口 `me.jayer.hdata.core.HData` 必须传 `--config=<TOML 路径>`（`HDataOptions.getConfig()` 标了 `@Required`）。
- 配置顶层为 `[[sources]] / [[transforms]] / [[sinks]]` 数组，每个块必须有 `type` 字段；`type` 通过 Java SPI（ServiceLoader）选择连接器。样例见 `hdata-core/src/test/resources/job.toml`。

## 扩展新连接器（约定）
- 实现 `me.jayer.hdata.core.spi.StructuredIOProvider`（`identifier` / `createSource` / `createSink`）。
- 必须在 `src/main/resources/META-INF/services/me.jayer.hdata.core.spi.StructuredIOProvider` 注册实现类，否则 `ServiceLoader` 找不到（hdata-jdbc 已示范）。
- 配置反序列化用 `ObjectMappers.getTomlObjectMapper()`（Jackson 3.x，`tools.jackson` 包，已关闭 `FAIL_ON_UNKNOWN_PROPERTIES`），由自定义 descriptor 类接收各字段。
- 新模块需加入根 pom 的 `<modules>` 并依赖 `hdata-core`；Kotlin 编译由 kotlin-maven-plugin 处理（src/main/java 不存在时的 "Source root doesn't exist" 警告无害）。

## 测试
- 目前没有实际测试代码，只有 `hdata-core/src/test/resources/job.toml` 一个样例配置；`mvn test` 基本只编译。
- 该样例配置含真实 MySQL 连接串，本地运行需可达的 MySQL 实例。

## 其他
- `README.md` 基本为空，文档以本文件与代码为准。
- 包名统一为 `me.jayer.hdata.*`。
