#!/usr/bin/env sh
# Runs a local Testcontainers suite while keeping mapped container endpoints off HTTP proxies.
set -eu

local_hosts='localhost,127.0.0.1,::1'
if [ -n "${NO_PROXY:-}" ]; then
    NO_PROXY="$NO_PROXY,$local_hosts"
else
    NO_PROXY="$local_hosts"
fi
if [ -n "${no_proxy:-}" ]; then
    no_proxy="$no_proxy,$local_hosts"
else
    no_proxy="$local_hosts"
fi
export NO_PROXY no_proxy

# AWS SDK v2's proxy discovery is not consistent across HTTP clients and can ignore NO_PROXY for
# a mapped Testcontainers endpoint. Integration tests are local-network tests, so do not expose
# their JVM to any proxy variables.
unset http_proxy https_proxy all_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY

# Rootless Podman exposes a Docker-compatible API but cannot run Testcontainers' privileged Ryuk
# cleanup helper. Containers in this repository are always closed by their test fixtures; disable Ryuk
# only when the selected Docker host is Podman's socket, keeping ordinary Docker cleanup unchanged.
case "${DOCKER_HOST:-}" in
    *podman.sock*)
        export TESTCONTAINERS_RYUK_DISABLED=true
        ;;
esac

maven_cmd=${MAVEN_CMD:-mvn}
case "$($maven_cmd --version 2>/dev/null | sed -n '1s/.* \([0-9]\+\)\..*/\1/p')" in
    3) ;;
    *)
        echo "MAVEN_CMD must point to a Maven 3.x executable (set MAVEN_CMD explicitly when mvn is Maven 4)." >&2
        exit 2
        ;;
esac

exec "$maven_cmd" -B -Pintegration-tests "$@"
