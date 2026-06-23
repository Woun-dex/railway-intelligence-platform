#!/usr/bin/env bash

set -euo pipefail

TOTAL=${1:-300000}
BATCH=${2:-2000}
WORKERS=${3:-8}
URL=${INGEST_URL:-http://localhost:8090/ingest/position/batch}

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

batches=$(( (TOTAL + BATCH - 1) / BATCH ))
echo "Generating $batches batch files of $BATCH events ($TOTAL total) ..."

# Pre-generate batch payloads (a JSON array of PositionEvents each).
# One awk process builds an entire batch file (vs. one per field), which keeps
# generation to ~batches subprocesses instead of TOTAL*2.
for ((b = 0; b < batches; b++)); do
  awk -v b="$b" -v batch="$BATCH" 'BEGIN{
    printf "[";
    for (i = 0; i < batch; i++) {
      tid   = (b * batch + i) % 5000;       # 5000 distinct trips -> all partitions
      lat   = 48.80 + ((i % 100) / 1000.0);
      lon   = 2.30  + ((i % 100) / 1000.0);
      delay = (b + i) % 600;
      if (i > 0) printf ",";
      printf "{\"trip_id\":\"TRIP-%d\",\"vehicle_id\":\"V%d\",\"lat\":%.5f,\"lon\":%.5f,\"delay_seconds\":%d}", \
        tid, tid, lat, lon, delay;
    }
    printf "]";
  }' > "$TMPDIR/batch_$b.json"
done

echo "Posting with $WORKERS parallel workers -> $URL"
start=$(date +%s.%N)

printf '%s\n' "$TMPDIR"/batch_*.json | \
  xargs -P "$WORKERS" -I {} curl -s -o /dev/null \
    -X POST "$URL" -H 'Content-Type: application/json' --data-binary @{}

end=$(date +%s.%N)
elapsed=$(awk "BEGIN{print $end - $start}")
rate=$(awk "BEGIN{printf \"%.0f\", $TOTAL / $elapsed}")

echo "-----------------------------------------"
echo "Sent     : $TOTAL events"
echo "Elapsed  : ${elapsed}s"
echo "Throughput: $rate msg/s"
echo "-----------------------------------------"
echo "(target: > 15000 msg/s)"
