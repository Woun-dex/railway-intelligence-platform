# Railway Intelligence Platform

Real-Time Transit Disruption Intelligence Platform — an event-driven system that
detects a rail disruption, computes its passenger impact, predicts escalation,
and delivers the right message to every screen within a hard real-time budget.

> **This repository is at Milestone 1: The Backbone** — infrastructure,
> high-throughput ingestion, and strict serialization contracts. The downstream
> engines (propagation, RAPTOR, STGCN, optimization, diffusion) arrive in later
> milestones; their event contracts are already fixed in `shared-schemas`.

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

## Teardown

```bash
make down     # stop containers, keep data
make nuke     # stop + wipe the Kafka volume
```
