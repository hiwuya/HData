#!/usr/bin/env sh
# Produces one external plugin JAR for the runner smoke scripts.
set -eu

output_dir="${1:-target/runner-smoke/plugins}"
mvn -B -pl hdata-runner-smoke-plugin -am package -DskipTests
mkdir -p "$output_dir"
cp hdata-runner-smoke-plugin/target/hdata-runner-smoke-plugin-*.jar "$output_dir/"
