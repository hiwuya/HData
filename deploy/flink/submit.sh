#!/usr/bin/env sh
# Submit a pipeline from the HData image to a Flink JobManager.
# Required environment: FLINK_MASTER. Optional: PIPELINE, STATE_CHECKPOINT_PATH, HData options.
set -eu

: "${FLINK_MASTER:?Set FLINK_MASTER, for example flink-jobmanager:8081}"
PIPELINE="${PIPELINE:-/pipeline/pipeline.yaml}"

if [ ! -r "$PIPELINE" ]; then
  echo "Pipeline file is not readable: $PIPELINE" >&2
  exit 66
fi

state_options=""
if [ -n "${STATE_CHECKPOINT_PATH:-}" ]; then
  # These are FlinkPipelineOptions. The path must be durable storage shared by every worker.
  state_options="--checkpointingInterval=${CHECKPOINTING_INTERVAL_MS:-60000} --checkpointingMode=EXACTLY_ONCE --stateBackend=rocksdb --stateBackendStoragePath=${STATE_CHECKPOINT_PATH} --externalizedCheckpointsEnabled=true --retainExternalizedCheckpointsOnCancellation=true"
fi

# shellcheck disable=SC2086
exec java ${JAVA_TOOL_OPTIONS:-} -cp '/opt/hdata/lib/*' me.jayer.hdata.core.HData \
  --pipeline="$PIPELINE" \
  --runner=FlinkRunner \
  --flinkMaster="$FLINK_MASTER" \
  ${state_options} \
  "$@"
