#!/usr/bin/env bash
#
# Download the full, real Île-de-France Mobilités (Transilien / RER + the rest of
# the network) GTFS feed for the real-scale graph. The graph engine reads the
# .zip directly and keeps only rail routes (route_type filter), so there is no
# need to unzip.
#
# The committed graph-engine-service/.../gtfs/transilien-sample is a trimmed REAL
# extract used by default and by the tests; this script is for running against the
# complete ~hundreds-of-stations network.
#
# Usage:
#   bash scripts/fetch-gtfs.sh
#   RAIL_GTFS_PATH=data/gtfs/IDFM-gtfs.zip make run-graph
#
set -euo pipefail

GTFS_URL="${GTFS_URL:-https://eu.ftp.opendatasoft.com/stif/GTFS/IDFM-gtfs.zip}"
DEST_DIR="${DEST_DIR:-data/gtfs}"
OUT="${DEST_DIR}/IDFM-gtfs.zip"

mkdir -p "${DEST_DIR}"

echo "Downloading GTFS feed:"
echo "  ${GTFS_URL}"
echo "  -> ${OUT}"
curl -fL --progress-bar "${GTFS_URL}" -o "${OUT}"

SIZE=$(du -h "${OUT}" | cut -f1)
echo "Done (${SIZE})."
echo
echo "Run the engine against the full network (rail/RER only by default):"
echo "  RAIL_GTFS_PATH=${OUT} make run-graph"
echo
echo "The feed contains metro/bus/tram too; to include them set e.g.:"
echo "  RAIL_GTFS_ROUTE_TYPES=0,1,2 RAIL_GTFS_PATH=${OUT} make run-graph"
echo "A large feed may need more heap: MAVEN_OPTS=-Xmx2g make run-graph"
