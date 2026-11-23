#!/bin/sh
set -eu

socket_path=${1:-/run/tradecore-consumer-uds/consumer-v2.sock}
expected_identity=${2:-xuanling:xuanling:600}
attempt=0
while [ "$attempt" -lt 400 ]; do
    if [ -S "$socket_path" ]; then
        identity=$(stat -Lc '%U:%G:%a' -- "$socket_path" 2>/dev/null || true)
        if [ "$identity" = "$expected_identity" ]; then
            exit 0
        fi
    fi
    attempt=$((attempt + 1))
    sleep 0.05
done

echo "Consumer UDS socket did not become ready with the required identity" >&2
exit 1
