#!/usr/bin/env sh
# Submit the external unbounded smoke plugin using the Spark distribution's spark-submit command.
# The command returns after submission; use the Spark UI to observe records and cancel the application.
set -eu

: "${SPARK_MASTER:?Set SPARK_MASTER, for example spark://master.example:7077}"
: "${HDATA_APP_JAR:?Set HDATA_APP_JAR to hdata-core's packaged jar}"
: "${HDATA_EXTRA_JARS:?Set HDATA_EXTRA_JARS to the comma-separated runtime jars built with -Pspark-runner}"

plugin_dir="target/runner-smoke/plugins"
deploy/runner-smoke/build-plugin.sh "$plugin_dir"
plugin_jar=$(find "$plugin_dir" -maxdepth 1 -name 'hdata-runner-smoke-plugin-*.jar' -type f | head -n 1)
manifest="${RUN_MANIFEST:-target/runner-smoke/spark-manifest.json}"
mkdir -p "$(dirname "$manifest")"

exec spark-submit \
  --class me.jayer.hdata.core.HData \
  --master "$SPARK_MASTER" \
  --deploy-mode "${SPARK_DEPLOY_MODE:-client}" \
  --jars "$HDATA_EXTRA_JARS,$plugin_jar" \
  "$HDATA_APP_JAR" \
  --pipeline=deploy/runner-smoke/pipeline.yaml \
  --pluginDirectories="$plugin_dir" \
  --runner=SparkRunner \
  --sparkMaster="$SPARK_MASTER" \
  --waitUntilFinish=false \
  --runManifest="$manifest" \
  "$@"
