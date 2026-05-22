#!/usr/bin/env zsh
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: ./run-simple-test.sh <ops-schedule.json|-> <base-config.json> [docId] [outDir]"
  exit 1
fi

ops_file="$1"
base_config="$2"
if [[ "$ops_file" != "-" && ! -f "$ops_file" ]]; then
  echo "Ops schedule not found: $ops_file"
  exit 1
fi
if [[ ! -f "$base_config" ]]; then
  echo "Base config not found: $base_config"
  exit 1
fi

doc_id="${3:-}"
out_dir="${4:-simple-test-output}"

mkdir -p "$out_dir"

# Client arrival control:
#   CLIENT_ARRIVAL_MODE=simultaneous|uniform|burst|poisson
#   CLIENT_ARRIVAL_SEED=<optional RNG seed for reproducibility>
#
# simultaneous:
#   STARTUP_DELAY_MS=<fixed delay applied to every client>
# uniform:
#   CLIENT_ARRIVAL_WINDOW_MS=<uniform random delay range [0, window]>
# burst:
#   CLIENT_BURST_SIZE=<clients per burst>
#   CLIENT_BURST_GAP_MS=<gap between bursts>
#   CLIENT_BURST_JITTER_MS=<random jitter within each burst>
# poisson:
#   CLIENT_ARRIVAL_MEAN_MS=<mean inter-arrival time in ms>
#
# Realtime experiment mode:
#   EXPERIMENT_MODE=test|realtime
#   (realtime can run without schedule: pass '-' as first arg)
#   EXPERIMENT_DURATION_MS=<wall-clock duration for realtime mode>
#   CLIENT_IDS=<comma-separated ids, optional>
#   CLIENT_COUNT=<client count when CLIENT_IDS is empty>
#   CLIENT_ID_PREFIX=<prefix used with CLIENT_COUNT>
#   CLIENT_OP_TIMING_MODE=simultaneous|uniform|burst|poisson
#   CLIENT_OP_TIMING_SEED=<optional RNG seed>
#   CLIENT_OP_MEAN_MS=<mean submit interval for poisson>
#   CLIENT_OP_WINDOW_MS=<uniform random submit interval upper bound>
#   CLIENT_OP_BURST_SIZE=<ops per burst>
#   CLIENT_OP_BURST_GAP_MS=<gap between bursts>
#   CLIENT_OP_BURST_JITTER_MS=<random jitter within bursts>
#
# The script writes startupDelayMs per client based on the selected arrival mode.

config_list=($(python3 - <<'PY' "$ops_file" "$base_config" "$doc_id" "$out_dir"
import json
import os
import random
import sys

ops_path = sys.argv[1]
base_path = sys.argv[2]
doc_id = sys.argv[3] if len(sys.argv) > 3 else ""
out_dir = sys.argv[4] if len(sys.argv) > 4 else "simple-test-output"

if ops_path == "-":
  ops = {"meta": {}, "peers": []}
else:
  with open(ops_path, "r", encoding="utf-8") as f:
    ops = json.load(f)
with open(base_path, "r", encoding="utf-8") as f:
    base = json.load(f)

if not doc_id:
    doc_id = base.get("docId", "doc_1")

meta = ops.get("meta", {})
peers = ops.get("peers", [])

experiment_mode = os.environ.get("EXPERIMENT_MODE", base.get("mode", "test")).strip().lower()
if not peers:
  if experiment_mode != "realtime":
    print("", end="")
    sys.exit(0)

  ids_env = os.environ.get("CLIENT_IDS", "").strip()
  if ids_env:
    ids = [s.strip() for s in ids_env.split(",") if s.strip()]
  else:
    count = int(os.environ.get("CLIENT_COUNT", str(base.get("clientCount", 1))))
    prefix = os.environ.get("CLIENT_ID_PREFIX", str(base.get("clientIdPrefix", "client")))
    count = max(1, count)
    ids = [f"{prefix}{i+1}" for i in range(count)]
  peers = [{"peerId": pid} for pid in ids]

os.makedirs(os.path.join(out_dir, "configs"), exist_ok=True)
os.makedirs(os.path.join(out_dir, "metrics"), exist_ok=True)

total_ops = meta.get("totalOps")
if total_ops is None:
    total_ops = len(ops.get("events", []))

startup_delay_ms = int(os.environ.get("STARTUP_DELAY_MS", "0"))
snapshot_enabled = os.environ.get("SNAPSHOT_ENABLED", "true").strip().lower() not in ("0", "false", "no", "off")
snapshot_interval_ms = int(os.environ.get("SNAPSHOT_INTERVAL_MS", base.get("snapshotIntervalMs", 60000)))
experiment_duration_ms = int(os.environ.get("EXPERIMENT_DURATION_MS", str(base.get("experimentDurationMs", 300000))))
op_timing_mode = os.environ.get("CLIENT_OP_TIMING_MODE", base.get("opTimingMode", "poisson")).strip().lower()
op_timing_seed = os.environ.get("CLIENT_OP_TIMING_SEED", str(base.get("opTimingSeed", "")))
op_mean_ms = int(os.environ.get("CLIENT_OP_MEAN_MS", str(base.get("opTimingMeanMs", 1000))))
op_window_ms = int(os.environ.get("CLIENT_OP_WINDOW_MS", str(base.get("opTimingWindowMs", 5000))))
op_burst_size = int(os.environ.get("CLIENT_OP_BURST_SIZE", str(base.get("opBurstSize", 3))))
op_burst_gap_ms = int(os.environ.get("CLIENT_OP_BURST_GAP_MS", str(base.get("opBurstGapMs", 5000))))
op_burst_jitter_ms = int(os.environ.get("CLIENT_OP_BURST_JITTER_MS", str(base.get("opBurstJitterMs", 200))))
realtime_drain_timeout_ms = int(os.environ.get("REALTIME_DRAIN_TIMEOUT_MS", str(base.get("realtimeDrainTimeoutMs", 30000))))
realtime_poll_ms = int(os.environ.get("REALTIME_POLL_MS", str(base.get("realtimePollMillis", 100))))

def optional_int(value, fallback):
  if value is None or value == "":
    return fallback
  return int(value)

arrival_mode = os.environ.get("CLIENT_ARRIVAL_MODE", base.get("clientArrivalMode", "simultaneous")).strip().lower()
arrival_seed = os.environ.get("CLIENT_ARRIVAL_SEED", str(base.get("clientArrivalSeed", "")))
arrival_mean_ms = int(os.environ.get("CLIENT_ARRIVAL_MEAN_MS", str(base.get("clientArrivalMeanMs", os.environ.get("CLIENT_ARRIVAL_RATE_MS", "1000")))))
arrival_window_ms = int(os.environ.get("CLIENT_ARRIVAL_WINDOW_MS", str(base.get("clientArrivalWindowMs", os.environ.get("CLIENT_ARRIVAL_RANGE_MS", "5000")))))
burst_size = int(os.environ.get("CLIENT_BURST_SIZE", str(base.get("clientBurstSize", 3))))
burst_gap_ms = int(os.environ.get("CLIENT_BURST_GAP_MS", str(base.get("clientBurstGapMs", 5000))))
burst_jitter_ms = int(os.environ.get("CLIENT_BURST_JITTER_MS", str(base.get("clientBurstJitterMs", 200))))

rng = random.Random(int(arrival_seed)) if arrival_seed is not None and arrival_seed != "" else random.Random()

def poisson_delays_ms(count: int, mean_ms: int):
  if count <= 0:
    return []
  if mean_ms <= 0:
    mean_ms = 1
  delays = [0]
  current = 0.0
  for _ in range(1, count):
    # Exponential inter-arrival time with mean = mean_ms.
    current += rng.expovariate(1.0 / float(mean_ms))
    delays.append(int(round(current)))
  return delays

def uniform_delays_ms(count: int, window_ms: int):
  if count <= 0:
    return []
  if window_ms < 0:
    window_ms = 0
  return [rng.randint(0, window_ms) for _ in range(count)]

def burst_delays_ms(count: int, burst_size: int, gap_ms: int, jitter_ms: int):
  if count <= 0:
    return []
  if burst_size <= 0:
    burst_size = 1
  if gap_ms < 0:
    gap_ms = 0
  if jitter_ms < 0:
    jitter_ms = 0
  delays = []
  for i in range(count):
    burst_index = i // burst_size
    base_delay = burst_index * gap_ms
    jitter = rng.randint(0, jitter_ms) if jitter_ms > 0 else 0
    delays.append(base_delay + jitter)
  return delays

def build_arrival_delays(count: int):
  if arrival_mode == "poisson":
    return poisson_delays_ms(count, arrival_mean_ms)
  if arrival_mode == "uniform":
    return uniform_delays_ms(count, arrival_window_ms)
  if arrival_mode == "burst":
    return burst_delays_ms(count, burst_size, burst_gap_ms, burst_jitter_ms)
  return [startup_delay_ms for _ in range(count)]

def build_test_config(peer, peer_delay):
  cfg = dict(base)
  cfg["mode"] = "test"
  cfg["clientId"] = peer.get("peerId")
  cfg["docId"] = doc_id
  cfg["totalOps"] = total_ops
  cfg["clocks"] = peer.get("clocks", [])
  cfg["types"] = peer.get("types", [])
  cfg["positions"] = peer.get("positions", [])
  cfg["values"] = peer.get("values", [])
  cfg["metricsEnabled"] = True
  cfg["metricsOutputPath"] = os.path.join(out_dir, "metrics", f"{peer['peerId']}.csv")
  cfg["startupDelayMs"] = peer_delay
  cfg["snapshotEnabled"] = snapshot_enabled
  cfg["snapshotIntervalMs"] = snapshot_interval_ms
  return cfg

def build_realtime_config(peer, peer_delay):
  cfg = dict(base)
  cfg["mode"] = "realtime"
  cfg["clientId"] = peer.get("peerId")
  cfg["docId"] = doc_id
  cfg["totalOps"] = total_ops
  cfg["metricsEnabled"] = True
  cfg["metricsOutputPath"] = os.path.join(out_dir, "metrics", f"{peer['peerId']}.csv")
  cfg["startupDelayMs"] = peer_delay
  cfg["snapshotEnabled"] = snapshot_enabled
  cfg["snapshotIntervalMs"] = snapshot_interval_ms
  cfg["experimentDurationMs"] = experiment_duration_ms
  cfg["opTimingMode"] = op_timing_mode
  cfg["opTimingSeed"] = op_timing_seed
  cfg["opTimingMeanMs"] = op_mean_ms
  cfg["opTimingWindowMs"] = op_window_ms
  cfg["opBurstSize"] = op_burst_size
  cfg["opBurstGapMs"] = op_burst_gap_ms
  cfg["opBurstJitterMs"] = op_burst_jitter_ms
  cfg["realtimeDrainTimeoutMs"] = realtime_drain_timeout_ms
  cfg["realtimePollMillis"] = realtime_poll_ms
  return cfg

peer_delays = build_arrival_delays(len(peers))

paths = []
for idx, peer in enumerate(peers):
  peer_id = peer.get("peerId")
  if not peer_id:
    continue
  peer_delay = peer_delays[idx] if idx < len(peer_delays) else startup_delay_ms
  if experiment_mode == "realtime":
    cfg = build_realtime_config(peer, peer_delay)
  else:
    cfg = build_test_config(peer, peer_delay)

  out_path = os.path.join(out_dir, "configs", f"{peer_id}.json")
  with open(out_path, "w", encoding="utf-8") as f:
    json.dump(cfg, f, indent=2)
  paths.append(out_path)

print("\n".join(paths))
PY
))

if [[ ${#config_list[@]} -eq 0 ]]; then
  echo "No peers found in $ops_file"
  exit 1
fi

mkdir -p "$out_dir/logs"

echo "Starting ${#config_list[@]} client(s) with docId=${doc_id:-from base config}"

declare -a pids
for cfg in ${config_list[@]}; do
  peer_id=$(basename "$cfg" .json)
  out_file="$out_dir/logs/${peer_id}.log"
  echo "Starting peer=$peer_id -> $out_file"
  ./gradlew run --args="--test $cfg" -q > "$out_file" 2>&1 &
  pids+=("$!")
done

status=0
for pid in ${pids[@]}; do
  if ! wait "$pid"; then
    status=1
  fi
done

if [[ $status -ne 0 ]]; then
  echo "Test finished with failures. Check $out_dir/logs/*.log"
  exit 1
fi

echo "Test finished successfully. Metrics in $out_dir/metrics/*.csv"
