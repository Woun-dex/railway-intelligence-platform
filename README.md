# Railway Intelligence Platform

Real-Time Transit Disruption Intelligence Platform — an event-driven system that
detects a rail disruption, computes its passenger impact, predicts escalation,
and delivers the right message to every screen within a hard real-time budget.

> **Milestones 1 & 2 are implemented.** Milestone 1 is the backbone —
> infrastructure, high-throughput ingestion, strict serialization contracts.
> Milestone 2 is **Engine 1** — the embedded-Hazelcast graph layer with
> deterministic delay propagation and a localized RAPTOR router, loaded from real
> Transilien GTFS. The remaining engines (STGCN, optimization, diffusion) arrive
> in later milestones; their event contracts are already fixed in `shared-schemas`.

---

## Milestone 1 — what's here

The "central nervous system" of the platform: telemetry ingestion with strict
data contracts and guaranteed **per-journey causal ordering**.

```
railway-intelligence-platform/
├── docker-compose.yml            # Kafka (KRaft) + Schema Registry + Redpanda Console
├── Makefile                      # build/run/validate shortcuts
├── scripts/loadgen.sh            # throughput load generator (DoD: >15k msg/s)
├── shared-schemas/               # single source of truth — Protobuf contracts
│   └── src/main/proto/
│       ├── telemetry.proto       # PositionEvent, SignallingEvent, IncidentEvent
│       ├── prediction.proto      # CascadeEvent, PredictionEvent (later milestones)
│       └── display.proto         # DecisionEvent, DisplayMessage (later milestones)
└── telemetry-ingestion-service/  # reactive ingestion gateway (Spring WebFlux)
    └── .../ingestion/
        ├── IngestionApplication.java
        ├── config/KafkaProducerConfig.java       # reactor-kafka senders, tuned
        ├── controller/TelemetryGatewayController.java
        ├── parser/TelemetryPayloadParser.java    # SIRI-ET / GTFS-RT normalizer
        ├── partitioner/TripIdPartitioner.java     # trip_id → partition (Np≥22)
        └── service/TelemetryIngestionService.java # publish + DLQ
```

### Design decisions

| Choice | Why |
|---|---|
| **Protobuf** (not Avro) | Matches the `.proto` contract layout; compact wire format; Schema-Registry enforced. |
| **Kafka in KRaft mode** | No Zookeeper — simpler single-node dev topology, same as the 3-broker target. |
| **`trip_id` partitioning** | All events for one journey land on one partition ⇒ causal order preserved across parallel consumers. |
| **reactor-kafka + WebFlux** | Fully reactive hot path; back-pressure flows socket → Kafka. |
| **Idempotent producer (`acks=all`)** | No duplicates, no loss under retries — backs the "zero dropped frames" DoD. |
| **DLQ for unparseable input** | A malformed frame is preserved (raw bytes + diagnostic headers) in `rail.raw.dlq`, never dropped. |

---

## Prerequisites

- JDK 17+, Maven 3.9+
- Docker + Docker Compose
- (for `make smoke` / `make load`) bash + curl — on Windows use **Git Bash** or WSL

---

## Quick start

```bash
# 1. Start the backbone (Kafka, Schema Registry, Console) and create topics
make up

# 2. Build everything (generates Java from .proto, runs nothing)
make build

# 3. Run the ingestion gateway (listens on :8090)
make run-ingestion
```

Open interfaces:
- **Redpanda Console** — http://localhost:8080 (topics, schemas, consumer groups)
- **Schema Registry** — http://localhost:8081/subjects
- **Ingestion health** — http://localhost:8090/actuator/health

### Topics created

| Topic | Partitions | Purpose |
|---|---|---|
| `rail.raw.position` | 22 | normalized PositionEvents |
| `rail.raw.signalling` | 22 | normalized SignallingEvents |
| `rail.raw.incident` | 22 | normalized IncidentEvents |
| `rail.raw.dlq` | 6 | dead-letter (unparseable payloads) |
| `rail.graph.cascade`, `rail.predictions.stgcn`, `rail.decisions.topk` | 22 | reserved for later milestones |

---

## HTTP API

| Method | Path | Body | Use |
|---|---|---|---|
| POST | `/ingest/position` | single JSON object | low-latency, exact-bytes DLQ on malformed input |
| POST | `/ingest/signalling` | single JSON object | " |
| POST | `/ingest/incident` | single JSON object | " |
| POST | `/ingest/{stream}/batch` | JSON **array** of objects | high-throughput load path |

The parser accepts both GTFS-RT-style (`trip_id`, `lat`/`lon`, `timestamp`) and
SIRI-ET-style (`VehicleJourneyRef`, nested `Position`, ISO-8601 `Delay`,
`RecordedAtTime`) payloads.

```bash
# valid → 202, lands in rail.raw.position
curl -X POST localhost:8090/ingest/position -H 'Content-Type: application/json' \
  -d '{"trip_id":"RER-E:T4471","vehicle_id":"V12","lat":48.8443,"lon":2.3743,"delay_seconds":120}'

# malformed → 202, lands in rail.raw.dlq with x-error-reason header
curl -X POST localhost:8090/ingest/position -H 'Content-Type: application/json' \
  -d '{"trip_id":"BROKEN", not json'
```

---

## Definition of Done — how to validate

| DoD criterion | How to check |
|---|---|
| **>15,000 msg/s, no dropped frames** | `make run-ingestion`, then `make load` (defaults to 300k events). Reports achieved msg/s; verify topic count in Console matches sent count. |
| **Zero handling exceptions; DLQ for bad input** | `make smoke` (sends one valid + one malformed). Inspect `rail.raw.dlq` in Console — the malformed payload is there with diagnostic headers; the service logged no exception. |
| **Schema evolution / backward-compat tests** | `make test` → `SchemaEvolutionTest` runs the Schema-Registry compatibility engine offline: additive field = compatible, field-type change = rejected. |

### Validation commands

```bash
make test     # unit tests: partitioner determinism + schema compat + parser
make smoke    # happy-path + DLQ in one shot
make load     # throughput benchmark  (bash scripts/loadgen.sh [TOTAL] [BATCH] [WORKERS])
make topics   # list topics
```

---

## Milestone 2 — Engine 1 (Graph Engine)

The memory-localized computation layer: an **embedded Hazelcast IMDG** holds the
rail topology as flat primitive arrays (CSR), loaded from the **real IDFM GTFS
feed** (Île-de-France Mobilités — RER, Transilien, TER). Two engines read it
directly from heap:

- **Engine 1a — delay propagation.** Consumes `rail.raw.position`, cascades each
  delay downstream through the event-activity network with running/dwell/transfer
  **slack absorption**, prioritized by an offline **PageRank** hub vector, and
  emits `CascadeEvent` to `rail.graph.cascade`.
- **Engine 1b — RAPTOR.** Round-based journey planner over the flat timetable,
  returning the Pareto frontier over `(arrival_time, transfers)`. Full-network
  recompute is **sub-millisecond** (DoD budget: < 12 ms).

```
graph-engine-service/
└── src/main/java/com/rail/platform/graph/
    ├── domain/model/        RailTopology (CSR flat arrays), RailTopologyBuilder, …
    ├── application/          DelayPropagationService (1a), RaptorRouter (1b), HubRankingService
    └── infrastructure/
        ├── topology/         GtfsTopologyLoader, SlackModel, HazelcastTopologyRepository
        ├── config/           HazelcastConfig, Kafka consumer/producer
        ├── messaging/        PositionEventConsumer, KafkaCascadePublisher
        └── web/              GraphMatrixController + static/viz.html
```

### Run it

```bash
make up            # infra (if not already running)
make build
make run-graph     # graph engine on :8091  (run-ingestion in another shell for the live flow)
```

- **Network viewer** — http://localhost:8091/viz.html (adjacency-matrix heatmap +
  geographic node-link graph + a RAPTOR journey form; node size = PageRank hub score)
- **Matrix JSON** — http://localhost:8091/graph/matrix (`make graph-matrix`)
- **Stats / top hubs** — http://localhost:8091/graph/stats
- **Station search** — `http://localhost:8091/graph/stations?q=defense&limit=10`
- **RAPTOR** — `http://localhost:8091/graph/plan?from=16&to=23&departure=25200`
- **Spectral matrix (STGCN)** — `http://localhost:8091/graph/spectral?form=scaled`
- **Health** — http://localhost:8091/actuator/health

#### STGCN-ready spectral matrix

`/graph/matrix` is the raw directed adjacency (for the viewer). The next engine
(STGCN / ChebNet) needs the **symmetric normalized Laplacian** rather than the
raw graph, so `/graph/spectral` exposes it directly (sparse COO):

| `form` | Matrix | Use |
|---|---|---|
| `adjacency` | `Â = D̃^{-1/2}(W+I)D̃^{-1/2}` | GCN renormalization trick |
| `laplacian` | `L = I − D^{-1/2} W D^{-1/2}` | normalized Laplacian |
| `scaled` (default) | `L̃ = (2/λmax)L − I` | **Chebyshev-ready** (spectrum in [-1,1]) |

The weighted adjacency `W` is the directed running graph symmetrized with a
Gaussian kernel on travel time (`w_ij = exp(−t_ij²/σ²)`, Yu et al. 2018), and
`λmax` is computed by power iteration. The STGCN service consumes `L̃` and runs
the Chebyshev recurrence `T_k(L̃) = 2·L̃·T_{k-1} − T_{k-2}`.

### See a cascade end-to-end

```bash
make run-ingestion          # shell 1  (:8090)
make run-graph              # shell 2  (:8091)
make cascade-demo           # posts a delayed PositionEvent at La Défense
# -> CascadeEvent records appear on rail.graph.cascade in Console (http://localhost:8080)
```

> Station indices are dynamic and depend on the loaded feed. Use the station
> search endpoint to find indices: `curl localhost:8091/graph/stations?q=defense`

### GTFS data source

The engine defaults to the **full IDFM GTFS feed** (`../data/gtfs/IDFM-gtfs.zip`,
~392 Transilien stations, 24 rail routes including RER A–E, Transilien
H/J/K/L/N/P/R/U/V, and regional TER lines). The feed should be placed at
`data/gtfs/IDFM-gtfs.zip` (download via `make fetch-gtfs`).

To use the small bundled 26-station sample instead:

```bash
RAIL_GTFS_PATH=classpath:gtfs/transilien-sample make run-graph
```

To include metro and tram networks alongside rail:

```bash
RAIL_GTFS_ROUTE_TYPES=0,1,2 make run-graph
```

### Definition of Done — validation

| DoD criterion | How to check |
|---|---|
| **Exact multi-transfer Pareto routing on a 100-station network** | `RaptorRouterTest` asserts the exact frontier `[(9000,0),(4000,1),(3000,2)]` plus Pareto invariants. |
| **Full-network RAPTOR recompute < 12 ms** | `RaptorBenchmarkTest` (tag `benchmark`) — measured **p99 ≈ 0.014 ms** on the mock network. |
| **Hazelcast partitions recover on node restart** | `HazelcastFaultInjectionTest` — 2-member cluster, crash a member, snapshot survives via backup and re-syncs to a restarted member. |
| **Real GTFS topology** | `GtfsTopologyLoaderTest` — parses the bundled extract (26 stations, 4 routes, CSR adjacency + foot transfers). Conditional IDFM integration tests run when the full feed is present. |

```bash
mvn -pl graph-engine-service test                 # all engine tests
mvn -pl graph-engine-service test -Dgroups=benchmark   # benchmark only
```

---

## Teardown

```bash
make down     # stop containers, keep data
make nuke     # stop + wipe the Kafka volume
```
