#!/usr/bin/env sh
# Submit the external unbounded smoke plugin to a reachable Flink JobManager.
# The command returns after submission; use the Flink UI/REST API to observe records and cancel the job.
set -eu

: "${FLINK_MASTER:?Set FLINK_MASTER, for example jobmanager.example:8081}"
: "${HDATA_CLASSPATH:?Set HDATA_CLASSPATH to the runtime classpath built with -Pflink-runner}"

plugin_dir="target/runner-smoke/plugins"
deploy/runner-smoke/build-plugin.sh "$plugin_dir"
manifest="${RUN_MANIFEST:-target/runner-smoke/flink-manifest.json}"
mkdir -p "$(dirname "$manifest")"

exec java ${JAVA_TOOL_OPTIONS:-} -cp "$HDATA_CLASSPATH" me.jayer.hdata.core.HData \
  --pipeline=deploy/runner-smoke/pipeline.yaml \
  --pluginDirectories="$plugin_dir" \
  --runner=FlinkRunner \
  --flinkMaster="$FLINK_MASTER" \
  --waitUntilFinish=false \
  --runManifest="$manifest" \
  "$@"
