# Deployment

HData ships as a set of plain Maven module jars (`hdata-core` + one jar per connector), not a single
uber/fat jar — there is no shade or assembly plugin in this repo, and that is deliberate: connector
modules pull in independent, sometimes version-sensitive dependency trees (Hadoop for Hive/Iceberg,
a separately-pinned Kafka client for Debezium, Spark's own Hadoop version, and so on — see the
comments throughout the poms). Merging everything into one jar would reintroduce exactly those
conflicts for pipelines that only need a fraction of the connectors. Instead, a deployable artifact
is **the classpath of just the modules one pipeline actually uses**, the same idea as the
[Quick start](../README.md#quick-start) example, produced reproducibly with the Maven dependency
plugin instead of hand-listing jars.

## 1. Building a deployable classpath

```bash
# 1. Build and install every module this pipeline needs into the local repo (hdata-core is always needed).
#    Example: a pipeline that only uses ReadFromJdbc/WriteToJdbc.
mvn -q -pl hdata-jdbc -am install -DskipTests

# 2. Resolve the full runtime classpath (Beam, the JDBC driver, everything) for those modules.
mvn -q -pl hdata-jdbc org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath \
  -Dmdep.outputFile=/tmp/hdata-jdbc.cp

# 3. Run it.
java -cp "hdata-core/target/classes:hdata-jdbc/target/classes:$(cat /tmp/hdata-jdbc.cp)" \
  me.jayer.hdata.core.HData --pipeline=examples/jdbc-to-jdbc.yaml --runner=DirectRunner
```

For a pipeline that uses several connectors, add each module to `-pl` (comma-separated) in both
steps 1 and 2, e.g. `-pl hdata-jdbc,hdata-kafka`; `build-classpath` resolves the union once you add
`-am`. Ship the resulting jars (module jars + everything `.cp` lists) to wherever the job actually
runs — a container image, a machine with `java` and network access to the target services, or a
cluster submission host — the classpath string is what every runner below is built from.

A CI/CD pipeline should run `mvn -q verify` first (unit tests, `-Pintegration-tests` if Docker is
available — see [CONTRIBUTING.md](../CONTRIBUTING.md)) and only build the classpath / ship artifacts
after that passes.

## 2. Choosing a runner

| Runner | Maven profile | Where Spark/Flink itself comes from | Typical use |
|---|---|---|---|
| DirectRunner | none (default) | n/a, in-process | Local development, `--dryRun` validation, smoke tests. Not sized for production throughput. |
| FlinkRunner | `-Pflink-runner` | bundled by `beam-runners-flink-2.2` (runtime scope) | Submitting to a real Flink cluster; see §3. |
| SparkRunner | `-Pspark-runner` | **provided by the cluster** at `spark-submit` time | Submitting to a real Spark cluster; see §4. |
| SparkRunner (local) | `-Pspark-runner,spark-local` | bundled (`spark-core`/`spark-streaming` added as runtime deps) | Running/testing SparkRunner with `java -cp` only, no cluster. |

Rebuild the classpath (§1) with the matching profile active, e.g.
`mvn -q -pl hdata-jdbc -am install -DskipTests -Pflink-runner`, before submitting to that runner.

## 3. Running on a Flink cluster

Beam's classic (non-portable) Java `FlinkRunner` talks to the JobManager directly — it does **not**
go through `flink run`. Point `--flinkMaster` at the cluster and submit like any other Java program;
Beam stages the classpath's jars to the cluster itself:

```bash
java -cp "hdata-core/target/classes:hdata-jdbc/target/classes:$(cat /tmp/hdata-jdbc.cp)" \
  me.jayer.hdata.core.HData \
  --pipeline=examples/jdbc-to-jdbc.yaml \
  --runner=FlinkRunner \
  --flinkMaster=jobmanager-host:8081 \
  --waitUntilFinish=false
```

`--waitUntilFinish=false` returns as soon as the job is accepted (useful for a streaming pipeline, or
when a submission host should not block); with the default `true`, the process blocks until the job
reaches a terminal state and exits `0` on `DONE`, `1` otherwise — safe to check from a shell/CI step.
Flink-specific tuning (parallelism, checkpointing, state backend, …) goes on the same command line as
additional `--` flags; they are `FlinkPipelineOptions`, not an HData concept — see the
[Beam FlinkRunner docs](https://beam.apache.org/documentation/runners/flink/) for the full option
list.

### 3a. Submitting via `flink run`

Alternatively, you can use Flink's own `flink run` command. This requires packaging the pipeline
into a fat jar (via the Maven shade or assembly plugin), because `flink run` expects a single jar
file rather than a classpath. HData's default build deliberately avoids shading (connector modules
have independent, sometimes version-conflicting dependency trees), so this approach is only suitable
when you control the shade configuration for the specific connectors your pipeline uses.

```bash
# 1. Build a shaded jar (requires a shade plugin configuration in your module's pom).
mvn -q package -Pflink-runner -DskipTests

# 2. Submit via flink run.
flink run \
  -c me.jayer.hdata.core.HData \
  -m jobmanager-host:8081 \
  target/hdata-pipeline-1.0.0-shaded.jar \
  --pipeline=examples/jdbc-to-jdbc.yaml \
  --runner=FlinkRunner \
  --flinkMaster=jobmanager-host:8081 \
  --waitUntilFinish=false
```

Note that `--flinkMaster` is still required even with `flink run`, because Beam's `FlinkRunner`
uses it to establish the connection to the JobManager. The `-m` flag tells `flink run` where the
JobManager is for jar distribution, while `--flinkMaster` tells Beam where to submit the
`JobGraph` — they serve different purposes and both must point to the same host.

## 4. Running on a Spark cluster

`beam-runners-spark-4` declares Spark itself `provided` — the cluster supplies it, so submission goes
through `spark-submit`, not `java -cp`. `spark-submit` takes one application jar positionally and a
comma-separated `--jars` list for the rest:

```bash
EXTRA_JARS=$(echo "hdata-core/target/hdata-core-1.0.0.jar,hdata-jdbc/target/hdata-jdbc-1.0.0.jar,$(cat /tmp/hdata-jdbc.cp)" | tr ':' ',')

spark-submit \
  --class me.jayer.hdata.core.HData \
  --master spark://spark-master-host:7077 \
  --deploy-mode cluster \
  --jars "$EXTRA_JARS" \
  hdata-core/target/hdata-core-1.0.0.jar \
  --pipeline=examples/jdbc-to-jdbc.yaml \
  --runner=SparkRunner \
  --sparkMaster=spark://spark-master-host:7077
```

The pipeline file must be reachable from wherever the driver actually runs (`--deploy-mode cluster`
runs the driver on a cluster node, not the submission host) — package it as a resource, put it on a
shared/distributed filesystem, or pass its contents another way; a bare local `--pipeline=./x.yaml`
path only works with `--deploy-mode client`. As with Flink, Spark-specific tuning
(`--executor-memory`, `--num-executors`, checkpoint location for streaming, …) is a normal
`spark-submit`/`SparkPipelineOptions` concern, not an HData one.

## 5. Secrets and environment-specific config

Never write credentials into a pipeline file. Every value in it can reference
`${VAR}` / `${VAR:-default}`, resolved from (in order) a `-D` system property, an environment
variable, then the inline default — see the "Conventions" section of
[docs/connectors.md](connectors.md). At deployment time that means:

- `java -DMYSQL_PASSWORD=... -cp ... me.jayer.hdata.core.HData ...` for a direct/Flink submission host;
- `spark-submit --conf spark.driver.extraJavaOptions=-DMYSQL_PASSWORD=... --conf spark.executor.extraJavaOptions=-DMYSQL_PASSWORD=...`
  for Spark, since the value has to reach whichever JVM actually binds the connector (driver for
  graph construction, executors for the DoFns that open real connections);
- or plain environment variables, set however the platform normally injects them (container env,
  systemd `EnvironmentFile`, an orchestrator's secret mount).

Substitution happens as a whole-file text replacement **before** YAML parsing, so it also matches
inside comments — do not rely on a comment to "disable" a placeholder.

## 6. Logging

The default config is [`hdata-core/src/main/resources/log4j2.xml`](../hdata-core/src/main/resources/log4j2.xml):
console output, `me.jayer.hdata` at `INFO`, everything else (including `org.apache.beam`) at `WARN`.
Since it ships inside `hdata-core.jar`, it is already part of every classpath built in §1 and applies
on cluster workers too, as long as `hdata-core.jar` is part of what gets shipped/staged there (true
for both the FlinkRunner and SparkRunner paths above).

Override it with `-Dlog4j2.configurationFile=/path/to/log4j2.xml`, passed the same way as any other
`-D` in §5 — on the submission host for Flink (Beam stages the file along with the classpath only if
it is itself included in the resolved jars/classpath; for a plain filesystem path, ship it to workers
through the cluster's own distribution mechanism instead, e.g. Flink's `--flinkConfDir`/`-yt` shipped
files or a baked-in container image), or via `spark.{driver,executor}.extraJavaOptions` for Spark.

## 7. Monitoring

**Job lifecycle.** `HData.main` returns exit code `0` only when the job reaches
`PipelineResult.State.DONE`; any other terminal state, or an `HDataException` (e.g. a config
validation failure caught before the graph even runs), returns `1`. That is enough for a
cron/CI/orchestrator step to treat the run as pass/fail without parsing logs. Use `--dryRun` in a
pre-flight step to validate a pipeline file (config binding + DAG construction) without touching any
real source or sink.

**Metrics.** Connectors report operational counters through Beam's `Metrics` API (namespaced by the
DoFn class, e.g. `me.jayer.hdata.elasticsearch6.Elasticsearch6WriteFn`), for example:

| Metric | Emitted by | Meaning |
|---|---|---|
| `records_read` | several read-side DoFns | rows successfully produced |
| `records_written` | several write-side DoFns | rows successfully written |
| `records_rejected` | write-side DoFns with dead-letter support | rows sent to `error_handling` instead of the sink |
| `records_per_jdbc_batch` | JDBC write | batch size actually flushed |
| `milliseconds_per_batch` | JDBC write | batch flush latency |
| `partitions_added` | Hive write | new Hive partitions registered in the metastore |
| `files_listed` / `files_deleted` | Hive / filesystem write (overwrite mode) | files discovered / removed during a table overwrite |
| `orcStripesSkipped` / `parquetRowGroupsSkipped` | Hive read (predicate/SYSTEM-sampling pushdown) | blocks skipped without being scanned |

How to read them depends on the runner:

- **DirectRunner**: call `result.metrics().queryMetrics(...)` on the `PipelineResult` (see
  `HivePipelineTest` for examples) — only useful when embedding HData as a library, not the `main` CLI.
- **FlinkRunner**: Beam metrics surface through Flink's own metrics system (accumulators/reporters);
  read them from the Flink Web UI or a configured Flink metrics reporter, the same place you would
  look for any other Flink job's metrics.
- **SparkRunner**: similarly surfaced through Spark's own metrics system; read them from the Spark UI
  or a configured Spark metrics sink.

`records_rejected` climbing when it normally sits at zero is usually the first useful alert to wire
up for a pipeline that has `error_handling` configured — see the "Dead letter" section of
[connectors.md](connectors.md#dead-letter-error_handling) for what lands in that stream and why.

## 8. Executable deployment templates

[`deploy/docker/Dockerfile`](../deploy/docker/Dockerfile) builds a non-root Java 17 runtime image for
exactly the connector modules a job needs. Select the modules at image-build time, mount a pipeline
file, and pass normal HData options at runtime:

```bash
docker build --build-arg MODULES=hdata-jdbc,hdata-kafka \
  -f deploy/docker/Dockerfile -t registry.example/hdata:jdbc-kafka .
docker run --rm \
  -v "$PWD/examples:/pipeline:ro" \
  -e MYSQL_PASSWORD \
  registry.example/hdata:jdbc-kafka \
  --pipeline=/pipeline/jdbc-to-jdbc.yaml --runner=DirectRunner
```

The Dockerfile copies only the selected connector jars and their runtime dependency closure into the
final image. Rebuild it whenever the selected modules or their resolved dependency versions change.
It intentionally does not bake a pipeline or credentials into an image.

[`deploy/flink/job.yaml`](../deploy/flink/job.yaml) is a Kubernetes `Job` template for submitting a
Flink job from that image. It mounts a pipeline `ConfigMap` read-only and obtains credentials from a
Kubernetes `Secret`; values such as `MYSQL_PASSWORD` remain ordinary `${MYSQL_PASSWORD}` placeholders
in the pipeline. The Job calls [`deploy/flink/submit.sh`](../deploy/flink/submit.sh), which requires
`FLINK_MASTER` and passes checkpoint options only when `STATE_CHECKPOINT_PATH` is set:

```bash
kubectl create configmap hdata-pipeline --from-file=pipeline.yaml=examples/jdbc-to-jdbc.yaml
kubectl create secret generic hdata-secrets --from-literal=mysql-password='replace-me'
kubectl apply -f deploy/flink/job.yaml
```

Set `STATE_CHECKPOINT_PATH` to durable storage reachable from all Flink workers before using an
unbounded source. The template enables exactly-once Flink checkpoints and retains externalized
checkpoints on cancellation, but a connector’s declared delivery contract still determines whether a
sink can avoid duplicates after replay. Inspect the resulting run manifest and the Flink JobManager
before promoting a workload to production.
