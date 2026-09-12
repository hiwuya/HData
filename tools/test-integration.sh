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

exec mvn -B -Pintegration-tests "$@"
