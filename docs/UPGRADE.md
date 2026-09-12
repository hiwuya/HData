# Upgrade guide

Before upgrading, run the normal test suite and `--dryRun` for every production pipeline. Keep the
previous artifact set and run manifest until the replacement job has passed its recovery test.

Current compatibility baseline: Java 17, Maven 3.9+, Beam 2.76.0, pipeline `format_version: 1`, and
connector `config_version: 1`. Future version changes will include a before/after YAML migration here.

For isolated plugins, rebuild against the supported `hdata-plugin-api` version, repackage with
`tools/package-plugin.sh`, verify `hdata-plugin.sha256`, and run the relevant runner smoke job.
