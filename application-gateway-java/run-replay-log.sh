#!/usr/bin/env zsh
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: ./run-replay-log.sh <generated-log.json>"
  exit 1
fi

log_file="$1"
if [[ ! -f "$log_file" ]]; then
  echo "Log file not found: $log_file"
  exit 1
fi

mkdir -p replay-logs

peer_ids=($(python3 - <<'PY' "$log_file"
import json, sys
p = sys.argv[1]
with open(p, 'r', encoding='utf-8') as f:
    data = json.load(f)
for peer in data.get('peers', []):
    pid = peer.get('peerId')
    if pid:
        print(pid)
PY
))

if [[ ${#peer_ids[@]} -eq 0 ]]; then
  echo "No peers found in $log_file"
  exit 1
fi

echo "Replaying log for peers: ${peer_ids[*]}"

declare -a pids
for peer_id in ${peer_ids[@]}; do
  out_file="replay-logs/${peer_id}.log"
  echo "Starting peer=$peer_id -> $out_file"
  ./gradlew run --args="--replay-log $log_file --peer $peer_id" -q > "$out_file" 2>&1 &
  pids+=("$!")
done

status=0
for pid in ${pids[@]}; do
  if ! wait "$pid"; then
    status=1
  fi
done

if [[ $status -ne 0 ]]; then
  echo "Replay finished with failures. Check replay-logs/*.log"
  exit 1
fi

echo "Replay finished successfully. Logs are in replay-logs/."
