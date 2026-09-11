<!-- Thanks for the PR! Please confirm the checklist below before merging. -->

## What changed

<!-- What does this PR do, why, and which issue does it relate to (e.g. #123)? -->

## Checklist

- [ ] `mvn -B verify` passes (CI will also run it)
- [ ] New/changed behavior has a matching test (end-to-end preferred; the logic layer should at least cover codec / split / config validation)
- [ ] Docs updated accordingly (`docs/connectors.md`, `README.md`, `AGENTS.md`, etc.)
- [ ] A new connector is registered in `META-INF/services/me.jayer.hdata.core.spi.TransformProvider`
- [ ] The dead-letter path uses `ErrorSchemas.failure(...)` and carries the original row's timestamp and window (`ValueInSingleWindow`)
- [ ] A config key either genuinely takes effect or fails `validate` explicitly (no silent fallback)
- [ ] The DoFn does not capture a non-serializable object (add a `SerializableUtils.ensureSerializable(...)` test where needed)

## Breaking changes

<!-- Any incompatible config / behavior change? If so, describe the migration path. -->
