#!/usr/bin/env sh
# Create reviewable, unsigned release evidence. Signing is performed by the release environment.
set -eu
output_dir="${1:-target/release-artifacts}"
mkdir -p "$output_dir"
output_find="./${output_dir#./}"
mvn -B -DskipTests package org.cyclonedx:cyclonedx-maven-plugin:2.9.1:makeAggregateBom
find . -path "$output_find" -prune -o -path '*/target/*.jar' -type f -exec cp {} "$output_dir"/ \;
find . -path "$output_find" -prune -o -path '*/target/bom.*' -type f -exec cp {} "$output_dir"/ \;
(cd "$output_dir" && sha256sum * > SHA256SUMS)
printf 'Unsigned artifacts written to %s\n' "$output_dir"
