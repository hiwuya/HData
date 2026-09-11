# HData Architecture Design

Architecture notes after a redesign that aligns with the [Beam Programming Guide](https://beam.apache.org/documentation/programming-guide/) and the [Beam YAML Specification](https://beam.apache.org/documentation/sdks/yaml/).

---

## 1. Problems before the refactor

### 1.1 The graph model could only express "a straight line" — and even the straight line was wrong

`HData.start()` before the refactor:

```kotlin
for (source in sources) {
    var input = pipeline.apply(source)
    transforms.forEach { input = input.apply(it) }
    sinks.forEach { input.apply(it) }
}
```

The config model was three flat arrays `sources / transforms / sinks`, so:

- **N sources × M sinks get fully connected**: two sources and two sinks yield 4 write paths,
  not the two the user wanted. The bigger the job, the easier to produce an unexpected graph.
- **Branch, merge, and join are all inexpressible**. Beam's core model is "PCollection as variable, PTransform as function",
  which can build an arbitrary DAG; the three-stage config cut this capability away.
- **A transform cannot apply to only one path** — all transforms are forced onto all sources.

### 1.2 The SPI welded source and sink into a single class

```kotlin
interface StructuredIOProvider {
    fun identifier(): String
    fun createSource(config: Map<String, Any>): StructuredSource
    fun createSink(config: Map<String, Any>): StructuredSink
}
```

A read-only connector (e.g. some HTTP data source) must implement a `createSink` that throws.
The read and write ends share one `identifier`, so config is squeezed into the same namespace.

### 1.3 The connector knew the config file's format

```kotlin
// JdbcStructuredIOProvider
val descriptor = ObjectMappers.getTomlObjectMapper().convertValue(config, JdbcSourceDescriptor::class.java)
```

The connector directly depends on `TomlMapper`. To switch formats, every connector had to be changed.

### 1.4 A bad config did not error

`FAIL_ON_UNKNOWN_PROPERTIES` was off: writing `partitionNum` as `partionNum` would be silently dropped,
and the job would keep running, just without partitioning — exactly the kind of problem that is hard to debug in production.

### 1.5 No error-handling channel

One dirty row that cannot be written to the target DB brings down the whole job. The dead-letter pattern from the Beam Programming Guide was not implemented.

### 1.6 Misc

- The package rename left behind many fully-qualified names (`me.jayer.hdata.jdbc.util.JdbcUtils.createDataSource(...)`), badly hurting readability.
- `beam-runners-flink` excluded all `org.apache.flink:*`, which caused `PipelineOptionsFactory`
  to **fail static initialization** when scanning `PipelineOptionsRegistrar` because `FlinkPipelineOptions` could not resolve the class,
  so the program could not even start.
- `JdbcTypeRegistry`'s branch for ARRAY columns still had a leftover `TODO("Not yet implemented")`, returning a
  handler object instead of an array value.
- `SqlRunner`'s vararg forwarding missed the spread operator (`query(it, sql, handler, args)`), so a parameterized query would treat the whole array as a single parameter.

---

## 2. Config format choice: TOML vs YAML

### 2.1 Conclusion

**The pipeline file supports YAML only, and directly adopts the Beam YAML dialect.**

The old TOML format is no longer supported — the benefit of co-existing formats (near zero, since both must parse into the same syntax tree anyway)
does not outweigh its cost: two equivalent samples must be maintained in sync, both styles must appear in docs,
and the user still has to decide which to use first. The parse result is still the format-agnostic `JsonNode`,
the connector does not perceive what the file looks like, and adding a format later is just registering one more mapper.

### 2.2 Why not TOML

TOML's design goal is "the most human-readable minimal config-file format", and it is excellent for **flat key-value config**
(`Cargo.toml`, `pyproject.toml` are both that shape). But a pipeline file describes a **DAG**,
which is exactly TOML's weakest scenario:

| Dimension | TOML | YAML |
|---|---|---|
| Nested structure | expressed via fully-qualified table headers; one deeper level adds one more prefix | indentation naturally expresses hierarchy |
| Array-of-object-of-array | `[[pipeline.transforms.transforms]]`, and order-sensitive | indentation suffices |
| Visual grouping | no boundaries between array elements; hard to tell nodes apart as they grow | `- ` makes node boundaries clear at a glance |
| Multi-line strings (SQL) | supports `"""` but watch escaping | `|` block literal, paste SQL verbatim |
| Reuse | none | anchors `&`/`*` (not yet enabled in this project) |
| Ecosystem | mostly language package managers | K8s / Argo / Airflow / CI are all YAML |
| Relation to official Beam | none | **Beam YAML is the official spec**, examples in the official docs can be copied directly |

The most intuitive example is the nested chain in `examples/branching.yaml`: written in TOML it becomes
`[[pipeline.transforms.transforms]]`, and one level deeper is basically unreadable; in YAML it is just one more indentation level.

TOML's strengths (unambiguous, no indentation sensitivity, no YAML 1.1 "Norway problem") do not weigh enough here:
a pipeline file is **structure-dominant**, not **value-dominant**.

### 2.3 Why follow the Beam YAML dialect

Beam's official YAML SDK is implemented in Python; this project re-implements the equivalent semantics in Kotlin.
The benefits of following the same dialect (`pipeline` / `type: chain` / `transforms` / `input` / `config` /
`error_handling` / `extra_transforms` / `windowing`):

1. User knowledge transfers, and example structures from official docs are directly usable;
2. Transform names follow the official ones (`ReadFromJdbc` / `WriteToJdbc` / `MapToFields` / `Flatten` /
   `LogForTesting` / `AssertEqual` / `StripErrorMetadata`);
3. Config keys use `snake_case`, consistent with the official SDK;
4. If we later want interoperability with the official YAML SDK (the same file runs on both sides), the gap is smallest.

**Differences from the official** (parts not yet supported by this project): UDF expressions via `language: python/javascript`,
the `Sql` transform, `providers` (cross-language extension service), `error_handling.threshold`.
Of these, `threshold` **errors explicitly** rather than being silently ignored.

---

## 3. The refactored architecture

### 3.1 Layers

```
hdata-core
├── spec/        syntax tree + parsing + variable substitution + structural validation for pipeline files (no Beam semantics)
├── spi/         connector extension points: HDataTransform / TransformProvider / TransformConfig
├── registry/    type -> provider registry (HData SPI + bridge to Beam native SchemaTransformProvider)
├── graph/       syntax tree -> Beam DAG builder (chain / composite / reference resolution / dead letter / window)
├── transforms/  built-in transforms
├── error/       schema convention for dead-letter records
├── util/        JsonNode <-> Beam Row conversion
└── HData.kt     entry point: parse args -> load file -> graph construction -> submit
```

The data flow is one-directional: `spec` does not know Beam, `graph` does not know the file format, and `transforms`/connectors know neither.

### 3.2 A unified transform contract: just use Beam's `SchemaTransform`

```kotlin
abstract class HDataTransform : SchemaTransform()
// i.e. PTransform<PCollectionRowTuple, PCollectionRowTuple>
```

This is the most critical step of the refactor. The old `StructuredSource` / `StructuredTransform` / `StructuredSink`
were three types with mismatched signatures (`PBegin->PCollection`, `PCollection->PCollection`, `PCollection->PDone`),
so the graph builder had to handle three types separately, and naturally ended up written as "source stage / transform stage / sink stage".

After switching to `PCollectionRowTuple -> PCollectionRowTuple`:

- **Only one node type**; the **cardinality** (0/1/N) of inputs/outputs distinguishes read / process / write, and the graph builder handles only one contract;
- Each node inherently supports **multiple inputs** (join, flatten) and **multiple outputs** (main output + dead letter);
- HData's connector **is a valid Beam `SchemaTransform`**, directly reusable by a native Beam Java pipeline;
- Conversely, any Beam-native `SchemaTransformProvider` on the classpath (e.g.
  `beam:schematransform:org.apache.beam:jdbc_read:v1`) can be written directly into the pipeline file via
  `BeamSchemaTransformAdapter`, without writing a connector for each one.

Everyday connector writing need not face the tuple directly; use the three convenience base classes:

```kotlin
abstract class RowSource   : HDataTransform() { protected abstract fun read(begin: PBegin): PCollection<Row> }
abstract class RowTransform: HDataTransform() { protected abstract fun transform(input: PCollection<Row>): PCollection<Row> }
abstract class RowSink     : HDataTransform() { protected abstract fun write(input: PCollection<Row>): PCollection<Row>? /* dead letter */ }
```

### 3.3 One provider handles only one type

```kotlin
interface TransformProvider {
    fun identifier(): String                       // the type in the pipeline file
    fun inputCollectionNames(): List<String>       // empty = read side; ["*"] = variadic (Flatten)
    fun outputCollectionNames(): List<String>      // empty = write side
    fun from(config: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple>
}
```

`ReadFromJdbc` and `WriteToJdbc` are two independent providers, each evolving its config separately,
and read-only connectors no longer need to write empty implementations. Port declarations are metadata the graph builder uses to validate the user's `input`.

### 3.4 Config binding: the connector does not perceive the file format

`TransformConfig` only exposes the format-agnostic syntax tree, bound to a Kotlin data class:

```kotlin
class JdbcReadProvider : TypedTransformProvider<JdbcReadConfig>(JdbcReadConfig::class.java) {
    override fun create(config: JdbcReadConfig, context: TransformConfig) = ...
}
```

Supporting changes:

- `snake_case` naming strategy: write `fetch_size` in the file, still `fetchSize` on the Kotlin side;
- **Enable `FAIL_ON_UNKNOWN_PROPERTIES`**: a misspelled key errors directly instead of being silently dropped;
- `error_handling` is stripped by the framework first (passed to connectors that need it via `context.errorHandling`),
  so config classes can safely run in strict mode.

### 3.5 Graph construction: reference-based DAG

`PipelineGraphBuilder` translates the syntax tree into a Beam DAG:

- **chain**: linear, input implicitly provided by the previous node; writing `input` is not allowed (errors directly if written);
- **composite**: arbitrary DAG, `input: name` / `input: {A: x, B: y}` / `input: [x, y]`;
  built after **topological sorting** by dependency, so write order does not matter; a cycle is detected and the cycle is printed;
- **reference syntax** `name.output_port`; the dead-letter stream is consumed via `name.<error_handling.output>`;
- **nesting**: `type: chain` / `type: composite` can appear as child nodes; each level is an independent namespace,
  and an inner level can look up names in the outer level; a child node references the composite node's own input via the reserved name `input`;
- **`source` / `sink` shorthand** and **`extra_transforms`** (mounting nodes outside the chain, typically used to consume the error stream);
- **window**: node-level `windowing` applies to its input, pipeline-level applies to the root node's output.

The node name also serves as Beam's transform name (with scope path when nested), so the DAG is readable in the runner UI.

### 3.6 Dead letter

`error_handling` is a block in the config, consistent with Beam YAML:

```yaml
- type: WriteToJdbc
  name: WriteOrders
  config:
    table: dws_order
    error_handling:
      output: rejected
```

Framework constraint: **once declared, it must be consumed**, otherwise graph construction errors directly.
This rule comes from Beam YAML and serves to eliminate "thought it was caught but actually lost".

The dead-letter record keeps the original row rather than serializing it into a string, so the error stream remains a schema-bearing `PCollection<Row>`:

```
element       ROW<original schema>  NULLABLE
error_type    STRING
error_message STRING                NULLABLE
transform     STRING
```

`StripErrorMetadata` can restore the original record for replay after fixing the data.

JDBC write-side strategy: on batch-write failure, if dead letter is enabled it **falls back to per-row writes** to locate bad data,
and only the rows that truly cannot be written go to the dead-letter stream; without dead letter it keeps the original behavior and throws directly.

### 3.7 Entry point and running

`HData.main` is now just four steps: parse command line → load pipeline file → graph construction → submit.
The `options:` in the file serve as defaults for Beam's `PipelineOptions`, with command-line args taking higher priority.

Newly added:

- `--dryRun`: graph construction only, then print the DAG, without submitting;
- `${VAR}` / `${VAR:-default}` variable substitution (explicit variable > system property > environment variable), to move secrets out of the config file.
  Variable names must match `[A-Za-z_][A-Za-z0-9_.]*`, so it will not accidentally harm JDBC's table-range syntax `${00-15}`;
- The runner becomes a Maven profile (`-Pflink-runner` / `-Pspark-runner`), defaulting to DirectRunner only,
  fixing the "program cannot start at all" problem from 1.6.

---

## 4. Coverage against the Beam Programming Guide

| Beam concept | Where it lands |
|---|---|
| Pipeline / PipelineOptions | `HData.build()`, `HDataOptions` + `options:` in the file |
| PCollection / Schema / Row | uniformly `PCollection<Row>` throughout, schema inferred by connectors |
| PTransform | `HDataTransform` (= Beam `SchemaTransform`) |
| Composite transform | `type: chain` / `type: composite`, nestable |
| Coder | relies on schema auto-derivation (`setRowSchema`), no custom Coder |
| DoFn lifecycle | JDBC read/write use `@Setup`/`@StartBundle`/`@ProcessElement`/`@FinishBundle`/`@Teardown` |
| Splittable DoFn | `JdbcSourceSplittableDoFn`, splitting `OffsetRange` by partition column |
| Multi-output / TupleTag | handled by the named ports of `PCollectionRowTuple` |
| dead-letter pattern | `error_handling` + `ErrorSchemas` + `StripErrorMetadata` |
| Window | `windowing:` (fixed / sliding / sessions / global) |
| Metrics | JDBC write-side batch size, batch elapsed, failed count |

**Not yet covered**: side input, GroupByKey/Combine-style aggregation, triggers and accumulation mode,
state & timer, cross-language transforms (`providers`). These are not needed in the main "data sync" scenario,
but the `SchemaTransform` contract itself poses no obstacle, so add them incrementally as transform needs arise.

---

## 5. What it takes to write a new connector

1. Implement `TransformProvider` (or `TypedTransformProvider<C>`), returning a
   `RowSource` / `RowTransform` / `RowSink`;
2. Register it in `META-INF/services/me.jayer.hdata.core.spi.TransformProvider`;
3. Add the new module to the root pom's `<modules>` and depend on `hdata-core`.

Refer to `hdata-jdbc` (full implementation) or `TestSinkProvider` in `hdata-core`'s test directory (minimal implementation, with dead letter).
