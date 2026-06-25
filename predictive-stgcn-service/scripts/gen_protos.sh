#!/usr/bin/env bash
# Generate Python protobuf bindings from the shared schema (single source of truth).
# Run from the predictive-stgcn-service/ directory.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROTO_SRC="${PROTO_SRC:-$HERE/../shared-schemas/src/main/proto}"
OUT="$HERE/src/proto"

if [ ! -d "$PROTO_SRC" ]; then
  echo "proto source dir not found: $PROTO_SRC" >&2
  exit 1
fi

mkdir -p "$OUT"
echo "Generating Python protobufs from $PROTO_SRC -> $OUT"
python -m grpc_tools.protoc -I "$PROTO_SRC" --python_out="$OUT" "$PROTO_SRC"/*.proto
echo "Done: $(ls "$OUT"/*_pb2.py | xargs -n1 basename | tr '\n' ' ')"
