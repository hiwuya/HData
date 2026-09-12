# Connector dependency isolation

All connector modules currently run in one application class path. Maven version
management keeps the selected versions consistent, but it cannot make two binary
incompatible versions of the same library coexist. This is already relevant for
client stacks that bring their own transitive dependencies, such as change-data
capture, search clients, and Hadoop-based file formats.

## Current release rules

Keep every shared dependency version in the root `dependencyManagement` section.
A connector must not override a shared version locally: Maven resolves one version
for the final application class path, so a local pin only hides the conflict in
that module's POM. The Kafka client and Connect runtime are one compatibility unit
and use the same root-managed `kafka.version` release line for this reason.

Before adding or upgrading a client library, inspect its resolved graph alongside
the connectors it can be installed with:

```bash
mvn -pl hdata-kafka,hdata-debezium -am dependency:tree -Dverbose \
  -Dincludes=org.apache.kafka:kafka-clients,org.apache.kafka:connect-runtime
```

Use exclusions and a root-managed version when the library is binary compatible.
Use an upstream shaded artifact only when it is designed for that purpose and its
relocated types do not cross the HData or Beam API. The ORC shaded-protobuf
artifacts are the established example. Do not add a general fat JAR: it would
make runner distribution and duplicate-class failures harder to diagnose.

## Target plugin boundary

Optional connectors should eventually be distributable as separate plugin
directories. Each directory contains a connector JAR, its private dependencies,
and a small descriptor with the HData API version and provider entry point. The
host discovers the descriptor, constructs one class loader per directory, and
uses `ServiceLoader` with that loader to find `TransformProvider` implementations.

The class loader must be child-first for private client libraries, while always
delegating these shared packages to the host:

- `java.*`, Kotlin runtime, and logging APIs;
- the HData plugin API and core SPI;
- Apache Beam APIs and their transitive libraries used in serialized pipeline
  objects;
- schema and configuration types that cross the provider boundary.

Provider APIs may exchange HData configuration values, Beam `Row` values, and
Beam transforms, but never client-library classes. Client instances remain inside
connector DoFns and are created in `@Setup`, then closed in `@Teardown`. This
keeps client libraries private while retaining Beam's serialization contract.

## Why this is staged

A connector transform is serialized by Beam and executed on worker processes.
Loading a plugin only in the launcher JVM is therefore insufficient: every
worker must receive the same plugin directory and resolve the provider and DoFn
classes through the same loader policy. DirectRunner, Flink, and Spark use
different artifact-staging and context-class-loader paths.

Introducing a plugin loader before that distribution contract exists would let a
job build successfully and then fail at worker deserialization. The migration
sequence is:

1. Keep the present reactor modules on a converged, root-managed class path and
   audit dependency trees when versions change.
2. Extract a deliberately small `hdata-plugin-api` artifact from core and add a
   plugin descriptor plus a deterministic loader test on DirectRunner.
3. Add runner-specific plugin staging and integration tests that execute a
   plugin-supplied DoFn on remote workers.
4. Move the highest-conflict optional connectors first, starting with the
   change-data-capture stack and incompatible search-client generations. Keep
   Hadoop, Beam, logging, and the HData API shared until runner tests prove a
   narrower boundary is safe.

The plugin descriptor must reject an incompatible HData API version before a
pipeline is constructed. A connector that cannot be isolated safely remains a
regular reactor module with explicit dependency convergence; isolation is a
deployment option, not a substitute for version management.
