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

maven_cmd=${MAVEN_CMD:-mvn}
case "$($maven_cmd --version 2>/dev/null | sed -n '1s/.* \([0-9]\+\)\..*/\1/p')" in
    3) ;;
    *)
        echo "MAVEN_CMD must point to a Maven 3.x executable (set MAVEN_CMD explicitly when mvn is Maven 4)." >&2
        exit 2
        ;;
esac

exec "$maven_cmd" -B -Pintegration-tests "$@"
