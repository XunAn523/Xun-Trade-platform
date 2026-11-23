#!/usr/bin/env bash
set -euo pipefail

# Operator-owned orchestration for the fixed Consumer UDS first-frame gate.
# The probes remain consumer-only; this wrapper owns reset and evidence capture.

: "${OUTPUT_ROOT:?set OUTPUT_ROOT to a new evidence directory}"
: "${TRADING_DAY:?set TRADING_DAY as ISO YYYY-MM-DD}"
: "${SERVICE_REVISION:?set SERVICE_REVISION to the deployed dev commit}"
: "${LOADGEN_REVISION:?set LOADGEN_REVISION to the deployed test commit}"

SERVICE_ROOT=${SERVICE_ROOT:-/opt/xuanling-tradecore}
LOADGEN_ROOT=${LOADGEN_ROOT:-/home/xuanling/tradecore-loadgen-72288c4}
CONFIG=${CONFIG:-/etc/xuanling-tradecore/consumer-uds.json}
UNIT=${UNIT:-xuanling-tradecore-consumer-uds.service}
PY=${PY:-$SERVICE_ROOT/.venv/bin/python}
ROUNDS=${ROUNDS:-"R1 R2 R3"}
SERVICE_CPUS=${SERVICE_CPUS:-"64-79,160-175"}
GENERATOR_CPUS=${GENERATOR_CPUS:-"32-40,48-59,80-95"}
S1_CPUS=${S1_CPUS:-"48-55"}
S2_CPUS=${S2_CPUS:-"56-59"}
S3_CPUS=${S3_CPUS:-"32-40,80-95"}
S1_PROCESSES=${S1_PROCESSES:-4}
S3_PROCESSES=${S3_PROCESSES:-25}
S1_WORKERS=${S1_WORKERS:-64}
S3_WORKERS=${S3_WORKERS:-200}
S3_QUEUE_CAPACITY=${S3_QUEUE_CAPACITY:-500}
S1_TPS=${S1_TPS:-2000}
S2_EPS=${S2_EPS:-5000}
S3_QPS=${S3_QPS:-5000}
BURST_COUNT=${BURST_COUNT:-5000}
BURST_LIMIT_MS=${BURST_LIMIT_MS:-0}
QUERY_BURST=${QUERY_BURST:-0}

test "$(git -C "$SERVICE_ROOT" rev-parse HEAD)" = "$SERVICE_REVISION"
test "$(git -C "$LOADGEN_ROOT" rev-parse HEAD)" = "$LOADGEN_REVISION"
test -z "$(git -C "$SERVICE_ROOT" status --porcelain)"
test -z "$(git -C "$LOADGEN_ROOT" status --porcelain)"
test ! -e "$OUTPUT_ROOT"
mkdir -p "$OUTPUT_ROOT"
ulimit -n 30000

capture_trace() {
    local destination=$1 status_path
    for _ in $(seq 1 100); do
        status_path=$(find /var/lib/tradecore-consumer-uds/traces -maxdepth 1 \
            -type f -name '*.status.json' -printf '%T@ %p\n' | sort -nr | sed -n '1p' | cut -d' ' -f2-)
        if "$PY" - "$status_path" <<'PY'
import json, sys
value = json.load(open(sys.argv[1], encoding="utf-8"))
raise SystemExit(0 if value["submitted"] == value["written"] else 1)
PY
        then
            break
        fi
        sleep .05
    done
    test -n "$status_path"
    cp "$status_path" "$destination/trace.status.json"
    cp "${status_path%.status.json}.jsonl" "$destination/trace.jsonl"
}

for ROUND in $ROUNDS; do
    OUT="$OUTPUT_ROOT/$ROUND"
    mkdir -p "$OUT"
    sudo -n systemctl restart "$UNIT"
    systemctl is-active --quiet "$UNIT"
    {
        date -u +%Y-%m-%dT%H:%M:%S.%NZ
        echo "service_revision=$SERVICE_REVISION"
        echo "loadgen_revision=$LOADGEN_REVISION"
        echo "service_cpus=$SERVICE_CPUS"
        echo "generator_cpus=$GENERATOR_CPUS"
        echo "s1_cpus=$S1_CPUS"
        echo "s2_cpus=$S2_CPUS"
        echo "s3_cpus=$S3_CPUS"
        echo "round=$ROUND"
        echo "s1_workers=$S1_WORKERS"
        echo "s1_processes=$S1_PROCESSES"
        echo "s3_processes=$S3_PROCESSES"
        echo "s3_workers=$S3_WORKERS"
        echo "s3_queue_capacity=$S3_QUEUE_CAPACITY"
        echo "target_rates=$S1_TPS/$S2_EPS/$S3_QPS"
        echo "burst_count=$BURST_COUNT"
        echo "burst_limit_ms=$BURST_LIMIT_MS"
        echo "query_burst=$QUERY_BURST"
        echo "generator_nofile=$(ulimit -n)"
        echo "generator_cgroup=$(cat /proc/self/cgroup)"
        taskset -pc $$
        uptime
        systemctl show "$UNIT" -p MainPID -p NRestarts -p AllowedCPUs -p MemoryMax \
            -p TasksMax -p MemoryCurrent -p MemoryPeak -p ActiveEnterTimestamp --no-pager
        ss -xln | grep '/run/tradecore-consumer-uds/consumer-v2.sock'
    } > "$OUT/environment-before.txt"

    taskset -c "$GENERATOR_CPUS" "$PY" "$LOADGEN_ROOT/tools/performance/consumer_uds_hot_probe.py" \
        --config "$CONFIG" --trading-day "$TRADING_DAY" --warmup 10000 --samples 10000 \
        --subround-size 1000 --timeout 5 --revision "$SERVICE_REVISION" --round "$ROUND" \
        --output-dir "$OUT/hot" > "$OUT/hot.stdout.json"

    # HOT contributed 20,000 accepted orders. Add 180,000 through the public
    # SubmitSignal contract so the frozen mixed-load fixture is exactly 200,000.
    # Fixture construction is outside the measured window; use the proven
    # sustainable rate so a host scheduling pause cannot create synthetic
    # GENERATOR_OVERLOADED gaps in the immutable snapshot cardinality.
    taskset -c "$GENERATOR_CPUS" "$PY" "$LOADGEN_ROOT/tools/performance/consumer_uds_steady_probe.py" \
        --config "$CONFIG" --trading-day "$TRADING_DAY" --rate 1500 --seconds 120 \
        --ramp 0 --max-inflight 1024 --timeout 5 --output "$OUT/seed" \
        > "$OUT/seed.stdout.json"
    "$PY" - "$OUT/hot/summary.json" "$OUT/seed/summary.json" <<'PY'
import json, sys
hot, seed = (json.load(open(path, encoding="utf-8")) for path in sys.argv[1:])
assert hot["passed"] is True and hot["accepted"] == 10_000
assert hot["warmup_statuses"] == {"ACCEPTED": 10_000}
assert seed["offered_measurement"] == 180_000
assert seed["measurement_statuses"] == {"ACCEPTED": 180_000}
assert seed["unique_accepted_lsn"] == 180_000
PY

    "$PY" - <<'PY' > "$OUT/snapshot-preparation.json"
import json, time, uuid
from pathlib import Path
from xuanling_tradecore.consumer import ConsumerUnixClientConfigV2, ConsumerUnixClientV2
from xuanling_tradecore.consumer.uds_server_v2 import load_consumer_uds_server_config_v2
c = load_consumer_uds_server_config_v2(Path('/etc/xuanling-tradecore/consumer-uds.json'))
client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(c.socket_path, 30))
run = uuid.uuid4().hex
started = time.monotonic_ns()
for attempt in range(1, 1201):
    response = client.create_order_snapshot(
        request_id=f'firstframe-{run}-{attempt}',
        consumer_account_id=c.consumer_account_id,
        page_size=100,
    )
    if response.get('status') == 'COMPLETED':
        payload = response['payload']
        assert payload['order_count'] == 200_000
        print(json.dumps({'attempts': attempt, 'build_ms': (time.monotonic_ns()-started)/1e6,
                          'order_count': payload['order_count']}))
        break
    if response.get('code') != 'SNAPSHOT_NOT_READY':
        raise RuntimeError(response)
    time.sleep(.02)
else:
    raise RuntimeError('snapshot preparation timeout')
PY

    taskset -c "$GENERATOR_CPUS" "$PY" "$LOADGEN_ROOT/tools/performance/consumer_uds_mixed_probe.py" \
        --config "$CONFIG" --trading-day "$TRADING_DAY" --duration 30 \
        --s1-tps "$S1_TPS" --s2-eps "$S2_EPS" --s3-qps "$S3_QPS" \
        --expected-orders 200000 --revision "$SERVICE_REVISION" --round "$ROUND" \
        --connection-mode fresh-per-request --s1-cpus "$S1_CPUS" --s2-cpus "$S2_CPUS" \
        --s3-cpus "$S3_CPUS" --s1-workers "$S1_WORKERS" --s3-workers "$S3_WORKERS" \
        --s1-processes "$S1_PROCESSES" --s3-processes "$S3_PROCESSES" \
        --s3-queue-capacity "$S3_QUEUE_CAPACITY" \
        --output-dir "$OUT/mixed" > "$OUT/mixed.stdout.json"
    # Do not fail fast here. Every formal round must retain burst, overload,
    # trace and memory evidence even when the consumer-observed mixed latency
    # gate fails. The aggregate verdict below remains fail-closed.
    "$PY" - "$OUT/mixed/summary.json" <<'PY'
import json, sys
mixed = json.load(open(sys.argv[1], encoding="utf-8"))
print(json.dumps({"mixed_passed": mixed["acceptance"]["passed"]}))
PY

    taskset -c "$GENERATOR_CPUS" "$PY" "$LOADGEN_ROOT/tools/performance/consumer_uds_burst_probe.py" \
        --config "$CONFIG" --trading-day "$TRADING_DAY" --count "$BURST_COUNT" --timeout 30 \
        --output "$OUT/burst-$BURST_COUNT.samples.ndjson" > "$OUT/burst-$BURST_COUNT.summary.json"
    if [ "$QUERY_BURST" = 1 ]; then
        taskset -c "$GENERATOR_CPUS" "$PY" "$LOADGEN_ROOT/tools/performance/consumer_uds_burst_probe.py" \
            --config "$CONFIG" --trading-day "$TRADING_DAY" --count "$BURST_COUNT" --timeout 30 \
            --operation ReadOrderSnapshotPage --output "$OUT/query-burst.samples.ndjson" \
            > "$OUT/query-burst.summary.json"
    fi
    taskset -c "$GENERATOR_CPUS" "$PY" "$LOADGEN_ROOT/tools/performance/consumer_uds_burst_probe.py" \
        --config "$CONFIG" --trading-day "$TRADING_DAY" --count 16384 --timeout 30 \
        --output "$OUT/overload-16384.samples.ndjson" > "$OUT/overload-16384.summary.json"

    "$PY" - "$OUT/burst-$BURST_COUNT.summary.json" "$OUT/overload-16384.summary.json" "$BURST_COUNT" <<'PY'
import json, sys
burst, overload = (json.load(open(path, encoding="utf-8")) for path in sys.argv[1:3])
assert burst["statuses"] == {"ACCEPTED": int(sys.argv[3])}
assert burst["unique_accepted_lsn"] == int(sys.argv[3])
assert overload["offered"] == 16_384
accepted = overload["statuses"].get("ACCEPTED", 0)
assert 0 < accepted <= 10_240
assert accepted < overload["offered"]
assert sum(overload["statuses"].values()) == overload["offered"]
PY

    sleep 1
    capture_trace "$OUT"
    systemctl show "$UNIT" -p MainPID -p NRestarts -p ActiveState -p Result \
        -p MemoryCurrent -p MemoryPeak -p TasksCurrent -p CPUUsageNSec --no-pager \
        > "$OUT/service-after.txt"
    cat "/sys/fs/cgroup/system.slice/$UNIT/memory.events" > "$OUT/memory.events.txt"
    "$PY" - "$OUT/trace.status.json" "$OUT/memory.events.txt" <<'PY'
import json, sys
trace = json.load(open(sys.argv[1], encoding="utf-8"))
events = dict(line.split() for line in open(sys.argv[2], encoding="utf-8") if line.strip())
assert trace["submitted"] == trace["written"]
assert trace["queue_dropped"] == 0 and trace["capacity_dropped"] == 0
assert trace["ingress"]["active_connections"] == 0
assert trace["ingress"]["preframe_connections"] == 0
assert trace["ingress"]["pending_connections"] == 0
assert trace["ingress"]["preframe_capacity"] == 8192
assert trace["ingress"]["total_connection_capacity"] == 10240
assert int(events.get("oom", 0)) == 0 and int(events.get("oom_kill", 0)) == 0
PY
done

sudo -n systemctl restart "$UNIT"
systemctl is-active --quiet "$UNIT"
taskset -c "$GENERATOR_CPUS" "$PY" "$LOADGEN_ROOT/tools/performance/consumer_uds_hot_probe.py" \
    --config "$CONFIG" --trading-day "$TRADING_DAY" --warmup 0 --samples 1 \
    --subround-size 1 --timeout 5 --revision "$SERVICE_REVISION" --round RECOVERY \
    --output-dir "$OUTPUT_ROOT/recovery" > "$OUTPUT_ROOT/recovery.stdout.json"
systemctl show "$UNIT" -p MainPID -p NRestarts -p ActiveState -p Result \
    -p MemoryCurrent -p MemoryPeak -p TasksCurrent --no-pager > "$OUTPUT_ROOT/service-recovered.txt"
"$PY" - "$OUTPUT_ROOT" "$BURST_COUNT" "$BURST_LIMIT_MS" "$QUERY_BURST" $ROUNDS <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
count, limit_ms, query_burst = int(sys.argv[2]), float(sys.argv[3]), sys.argv[4] == "1"
rounds, details = {}, {}
for name in sys.argv[5:]:
    folder = root / name
    mixed = json.load(open(folder / "mixed" / "summary.json", encoding="utf-8"))
    burst = json.load(open(folder / f"burst-{count}.summary.json", encoding="utf-8"))
    checks = {
        "mixed": mixed["acceptance"]["passed"],
        "burst_count": burst["statuses"] == {"ACCEPTED": count}
            and burst["unique_accepted_lsn"] == count,
        "burst_wall": limit_ms <= 0 or burst["batch_wall_ms"] < limit_ms,
    }
    if query_burst:
        query = json.load(open(folder / "query-burst.summary.json", encoding="utf-8"))
        checks["query_count"] = query["statuses"] == {"COMPLETED": count}
        checks["query_wall"] = limit_ms <= 0 or query["batch_wall_ms"] < limit_ms
    details[name] = checks
    rounds[name] = all(checks.values())
result = {"rounds": rounds, "passed": all(rounds.values())}
result["checks"] = details
(root / "overall.json").write_text(
    json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
if not result["passed"]:
    raise SystemExit("one or more formal mixed/burst rounds failed")
PY
echo "$OUTPUT_ROOT"
