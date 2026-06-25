# Predictive STGCN Service — Engine 3

A predictive intelligence layer that estimates **spatio-temporal delay
escalation risk** without touching the deterministic pipeline. It consumes
processed telemetry off the Kafka backbone, runs a Spatio-Temporal Graph
Convolutional Network over a rolling window of per-station delays, and streams an
escalation metric `I_P` back onto the bus for the Java criticality joiner.

```
 rail.raw.position ──▶ [STGCN service] ──▶ rail.predictions.stgcn ──▶ [Java joiner] ──▶ rail.criticality.scored
 (PositionEvent)        FastAPI + PyTorch     (PredictionEvent)         cascade ⋈ pred       (CriticalityEvent)
                              ▲
                              │ GET /graph/spectral?form=scaled  (Chebyshev-rescaled Laplacian L̃, sparse COO)
                              └────────────── graph-engine-service (:8091)
```

## Why it stays out of the critical path

The deterministic cascade (Engine 1a) is untouched. This service is a *parallel
subscriber*: it reads the same telemetry, predicts independently, and publishes to
its **own** topic. If it falls behind or dies, the deterministic pipeline is
unaffected and the joiner degrades gracefully to cascade-only scores.

## Architecture

| Component | File | Responsibility |
|-----------|------|----------------|
| FastAPI runtime | `src/main.py` | lifecycle, `/health`, `/metrics`, `/predict` probe |
| STGCN network | `src/model/stgcn_network.py` | ST-Conv blocks (temporal gated conv + Chebyshev graph conv) |
| Inference engine | `src/model/inference.py` | rolling `[T,N]` buffer, forward pass, `I_P` metric |
| Topology client | `src/graph/topology_client.py` | fetches L̃ → SciPy CSR/COO + torch sparse tensor |
| Async broker | `src/kafka/async_broker.py` | consumer + inference threads, protobuf/SR wire format |

### Graph adjacency (the "optimized format")

Rather than re-derive the Laplacian, the service fetches the **Chebyshev-rescaled
Laplacian** `L̃ = (2/λmax)·L − I` that the graph engine already computes
(`GET /graph/spectral?form=scaled`) as a sparse COO payload, and materializes it
as `scipy.sparse` CSR/COO **and** a `torch.sparse` tensor. Single source of truth
for the graph ⇒ the predictor's spatial operator always matches the deterministic
engine's station indexing and `graph_version`.

### STGCN

Faithful to Yu, Yin & Zhu (IJCAI 2018): two ST-Conv blocks, each a
`temporal-gated-conv → Chebyshev-graph-conv → temporal-gated-conv` sandwich, then
a head that collapses time to a per-station scalar forecast. The escalation metric:

```
I_P = clip( (δ̂_horizon − δ_now) / max(δ_now, ε), 0, 1 )
```

Weights are random-initialized unless `STGCN_CHECKPOINT_PATH` points at a
`state_dict`; under-observed (cold) stations fall back to a persistence heuristic
and set `fallback_used=true`.

## Run

### Docker (recommended — bundles protobuf generation + CPU torch)
```bash
# from the repo root; needs the backbone up (make up) and graph engine running (make run-graph)
docker compose --profile ml up --build stgcn-predictor
curl -s localhost:8092/health | jq
```

### Local
```bash
cd predictive-stgcn-service
python -m venv .venv && source .venv/bin/activate     # Python 3.11 (torch has no 3.14 wheels yet)
pip install --extra-index-url https://download.pytorch.org/whl/cpu -r requirements.txt
bash scripts/gen_protos.sh                            # generate *_pb2 from shared-schemas
python -m uvicorn src.main:app --port 8092
```

## Validate the DoD

**Inference latency P99 ≤ 35 ms** — pure model path:
```bash
pytest tests/test_stgcn_latency.py -s         # asserts P99 ≤ 35ms over N=392
```
…and under simulated concurrent load against the live service:
```bash
python scripts/loadgen.py --clients 16 --rounds 50 --runs 20 --budget-ms 35
```

**Model shape / metric sanity:**
```bash
pytest tests/test_model_shapes.py
```

## Configuration (env, prefix `STGCN_`)

| Var | Default | Meaning |
|-----|---------|---------|
| `STGCN_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka |
| `STGCN_SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Confluent SR |
| `STGCN_GRAPH_ENGINE_URL` | `http://localhost:8091` | topology / L̃ source |
| `STGCN_TELEMETRY_TOPIC` | `rail.raw.position` | processed telemetry in |
| `STGCN_PREDICTION_TOPIC` | `rail.predictions.stgcn` | forecasts out |
| `STGCN_HISTORY_STEPS` | `12` | temporal window length T |
| `STGCN_HORIZON_SECONDS` | `1800` | forecast horizon (15–45 min band) |
| `STGCN_CHEB_ORDER` | `3` | Chebyshev order K |
| `STGCN_INFERENCE_INTERVAL_MS` | `1000` | rolling forward cadence |
| `STGCN_CHECKPOINT_PATH` | _(empty)_ | optional trained `state_dict` |

> **Topic naming.** The milestone refers to the input as `rail.telemetry.processed`;
> the concrete normalized stream that carries data is `rail.raw.position`
> (`PositionEvent`), which is the default. The output topic
> `rail.predictions.stgcn` is the one provisioned by `docker-compose` and consumed
> by the Java joiner. Both are env-overridable.
