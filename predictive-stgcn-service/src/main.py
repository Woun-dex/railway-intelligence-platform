"""FastAPI runtime for the predictive STGCN service (Engine 3).

Boots the model against the live graph topology, starts the Kafka brokerage, and
exposes health / metrics / a synchronous inference probe. Topology fetch is
retried with backoff because the graph engine may still be loading GTFS when this
service starts.
"""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

from src.config import settings
from src.graph.topology_client import fetch_topology
from src.kafka.async_broker import AsyncBroker
from src.model.inference import InferenceEngine

logging.basicConfig(level=settings.log_level,
                    format="%(asctime)s %(levelname)s %(name)s — %(message)s")
log = logging.getLogger("stgcn.main")

INFER_HIST = Histogram("stgcn_inference_ms", "STGCN forward latency (ms)",
                       buckets=(1, 2, 5, 10, 15, 20, 25, 30, 35, 50, 100))
FRAMES = Counter("stgcn_frames_consumed_total", "Telemetry frames consumed")
PREDS = Counter("stgcn_predictions_emitted_total", "Prediction events emitted")
P99 = Gauge("stgcn_inference_p99_ms", "Rolling P99 inference latency (ms)")

STATE: dict[str, object] = {"engine": None, "broker": None, "ready": False}


async def _bootstrap() -> None:
    for attempt in range(1, 61):
        try:
            topo = await asyncio.to_thread(fetch_topology, settings.graph_engine_url)
            break
        except Exception as exc:  # noqa: BLE001 — engine still warming up
            log.warning("topology not ready (attempt %d): %s", attempt, exc)
            await asyncio.sleep(2.0)
    else:
        log.error("giving up fetching topology; service stays not-ready")
        return

    engine = InferenceEngine(topo, settings)
    # warm the graph/JIT paths so the first real inference isn't an outlier
    engine.forward_latency_ms(runs=3)
    broker = AsyncBroker(engine, settings)
    broker.start()
    STATE.update(engine=engine, broker=broker, ready=True)
    log.info("STGCN service ready (n=%d, model=%s)", topo.n, settings.model_version)


@asynccontextmanager
async def lifespan(_: FastAPI):
    task = asyncio.create_task(_bootstrap())
    try:
        yield
    finally:
        task.cancel()
        broker = STATE.get("broker")
        if broker:
            broker.stop()


app = FastAPI(title="Predictive STGCN Service", version=settings.model_version, lifespan=lifespan)


@app.get("/health")
async def health():
    broker: AsyncBroker = STATE.get("broker")  # type: ignore[assignment]
    engine: InferenceEngine = STATE.get("engine")  # type: ignore[assignment]
    return {
        "status": "ready" if STATE["ready"] else "starting",
        "nodes": engine.n if engine else 0,
        "graph_version": engine.topo.graph_version if engine else None,
        "frames_consumed": broker.frames_consumed if broker else 0,
        "predictions_emitted": broker.predictions_emitted if broker else 0,
        "last_inference_ms": round(broker.last_infer_ms, 3) if broker else 0.0,
        "p99_inference_ms": round(broker.p99_infer_ms(), 3) if broker else 0.0,
    }


@app.get("/metrics")
async def metrics():
    broker: AsyncBroker = STATE.get("broker")  # type: ignore[assignment]
    if broker:
        FRAMES._value.set(broker.frames_consumed)   # type: ignore[attr-defined]
        PREDS._value.set(broker.predictions_emitted)  # type: ignore[attr-defined]
        P99.set(broker.p99_infer_ms())
    return PlainTextResponse(generate_latest().decode(), media_type=CONTENT_TYPE_LATEST)


@app.post("/predict")
async def predict(runs: int = 20):
    """Synchronous inference probe — runs ``runs`` forwards over the current window
    and returns the latency distribution. Backs the load test / DoD latency check."""
    engine: InferenceEngine = STATE.get("engine")  # type: ignore[assignment]
    if not engine:
        raise HTTPException(503, "model not ready")
    samples = []
    for _ in range(max(1, runs)):
        ms = await asyncio.to_thread(engine.forward_latency_ms, 1)
        INFER_HIST.observe(ms)
        samples.append(ms)
    samples.sort()
    n = len(samples)
    return {
        "runs": n,
        "nodes": engine.n,
        "mean_ms": round(sum(samples) / n, 3),
        "p50_ms": round(samples[n // 2], 3),
        "p95_ms": round(samples[min(n - 1, int(0.95 * n))], 3),
        "p99_ms": round(samples[min(n - 1, int(0.99 * n))], 3),
        "max_ms": round(samples[-1], 3),
    }


@app.get("/")
async def root():
    return {"service": "predictive-stgcn", "version": settings.model_version,
            "ready": STATE["ready"], "ts": int(time.time() * 1000)}


def main() -> None:
    import uvicorn
    uvicorn.run("src.main:app", host=settings.host, port=settings.port,
                log_level=settings.log_level.lower())


if __name__ == "__main__":
    main()
