#!/bin/sh
set -eu

if [ "$#" -eq 0 ]; then
  echo "Usage: mount a pipeline at /pipeline/pipeline.yaml or pass HData options as arguments." >&2
  exit 64
fi

exec java ${JAVA_TOOL_OPTIONS:-} -cp '/opt/hdata/lib/*' me.jayer.hdata.core.HData "$@"
