# Pipeline and connector configuration compatibility

HData YAML has two explicit, independently checked compatibility numbers. Both default to **1** so
existing pipeline files need no edit.

```yaml
format_version: 1
pipeline:
  type: chain
  transforms:
    - type: ReadFromJdbc
      config:
        config_version: 1
        url: jdbc:postgresql://db/orders
        table: orders
```

`format_version` controls the top-level graph language: pipeline structure, execution settings,
options, transform references, and error-handling syntax. `config_version` controls the configuration
object of the individual transform. HData rejects zero, future, or non-integer versions before it
constructs a Beam pipeline; it does not guess how to interpret a newer document.

## Evolution rules

1. Additive optional fields may be introduced within version 1 only when their default preserves
   prior behavior. Each accepted option must have a behavior test.
2. Renaming, changing a field type or default, altering delivery/replay behavior, or changing graph
   semantics requires a new format/config version and an explicit migration note.
3. Old versions remain supported only while documented. Removing a version requires a deprecation
   notice in release notes and an upgrade guide that includes before/after YAML.
4. Unknown fields remain errors. This catches spelling mistakes and prevents a newer file from being
   silently run with an older interpretation.

Use `--dryRun` after an upgrade to bind all connector configurations and construct the graph without
submitting the resulting Beam pipeline. Connectors that infer schema or split metadata may contact a
source during construction; record the versioned file alongside the run manifest.
