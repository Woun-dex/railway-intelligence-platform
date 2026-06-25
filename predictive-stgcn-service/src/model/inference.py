"""Inference engine: rolling temporal buffer + STGCN forward + I_P metric.

Holds a ``[T, N]`` ring buffer of recent per-station delays fed by the telemetry
stream. Each `step_and_infer()` pushes the current frame, runs one batched STGCN
forward over the whole network, and emits a `Prediction` for every station whose
*current* delay is operationally relevant (above a floor), carrying the predicted
escalation metric

    I_P = clip( (δ̂_horizon − δ_now) / max(δ_now, ε), 0, 1 )

which the Java joiner folds into the Criticality Score. Cold stations (fewer than
``min_warm_steps`` observations) fall back to a persistence heuristic and set
``fallback_used = True`` so downstream consumers can discount them.

Thread-safety: `observe()` (called from the Kafka consumer thread) and
`step_and_infer()` (the inference cadence) are guarded by a single lock; the
torch forward runs outside the lock on a detached snapshot to avoid blocking
ingestion.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass

import numpy as np
import torch

from src.config import Settings
from src.graph.topology_client import Topology
from src.model.stgcn_network import STGCN

log = logging.getLogger("stgcn.inference")

# Stations below this current delay (seconds) are not worth a prediction event.
RELEVANT_DELAY_FLOOR_S = 60.0
_EPS = 30.0  # seconds; floors the denominator so tiny delays don't blow up I_P


@dataclass
class Prediction:
    station_id: int
    trip_id: str
    current_delay_s: float
    horizon_seconds: int
    median_s: float
    p90_s: float
    p95_s: float
    escalation_ratio: float
    fallback_used: bool
    inference_time_ms: float


class InferenceEngine:
    def __init__(self, topo: Topology, settings: Settings):
        self.topo = topo
        self.cfg = settings
        self.n = topo.n
        torch.set_num_threads(max(1, settings.torch_threads))

        self.model = STGCN(
            n_nodes=topo.n,
            c_in=1,
            hidden=settings.hidden_channels,
            order=settings.cheb_order,
            kt=3,
            history=settings.history_steps,
        ).eval()
        self.scale = settings.delay_scale
        self.trained = False
        ckpt = settings.checkpoint_path
        if ckpt and os.path.exists(ckpt):
            state = torch.load(ckpt, map_location="cpu")
            self.model.load_state_dict(state)
            self.trained = True
            log.info("loaded trained STGCN checkpoint from %s", ckpt)
        else:
            log.warning("no checkpoint at '%s' — model is untrained; every station "
                        "will use the persistence-heuristic fallback (fallback_used=true)", ckpt)
        self.l_tilde = topo.l_tilde_torch

        self._lock = threading.Lock()
        self._buffer = np.zeros((settings.history_steps, topo.n), dtype=np.float32)
        self._current = np.zeros(topo.n, dtype=np.float32)
        self._seen = np.zeros(topo.n, dtype=np.int32)
        self._last_trip: dict[int, str] = {}
        self._restore()

    # -- ingestion -----------------------------------------------------------
    def observe(self, station_id: int, delay_seconds: float, trip_id: str) -> None:
        if station_id < 0 or station_id >= self.n:
            return
        with self._lock:
            self._current[station_id] = float(delay_seconds)
            if trip_id:
                self._last_trip[station_id] = trip_id

    # -- inference -----------------------------------------------------------
    def step_and_infer(self) -> list[Prediction]:
        """Advance the temporal buffer by one frame and run a batched forward."""
        with self._lock:
            self._buffer = np.roll(self._buffer, -1, axis=0)
            self._buffer[-1] = self._current
            self._seen = np.minimum(self._seen + (self._current != 0), self.cfg.history_steps + 1)
            window = self._buffer.copy()
            current = self._current.copy()
            seen = self._seen.copy()
            last_trip = dict(self._last_trip)

        x = torch.from_numpy(window / self.scale).view(1, 1, self.cfg.history_steps, self.n)
        t0 = time.perf_counter()
        with torch.no_grad():
            forecast = self.model(x, self.l_tilde).view(-1).numpy() * self.scale  # residual seconds [N]
        infer_ms = (time.perf_counter() - t0) * 1000.0

        preds: list[Prediction] = []
        warm = self.cfg.min_warm_steps
        horizon = self.cfg.horizon_seconds
        for i in range(self.n):
            delta_now = float(current[i])
            if delta_now < RELEVANT_DELAY_FLOOR_S:
                continue
            # Use the net only when it is trained AND the node is warm; otherwise the
            # forecast is meaningless, so fall back to a persistence-with-drift heuristic.
            cold = (not self.trained) or (seen[i] < warm)
            if cold:
                delta_hat = delta_now * 1.10
            else:
                # The net predicts the horizon residual δ(t+h)−δ(t); anchor it to now.
                delta_hat = delta_now + float(forecast[i])
            delta_hat = max(0.0, delta_hat)
            i_p = (delta_hat - delta_now) / max(delta_now, _EPS)
            i_p = float(np.clip(i_p, 0.0, 1.0))
            preds.append(Prediction(
                station_id=i,
                trip_id=last_trip.get(i, ""),
                current_delay_s=delta_now,
                horizon_seconds=horizon,
                median_s=delta_hat,
                p90_s=delta_hat * 1.25,
                p95_s=delta_hat * 1.40,
                escalation_ratio=i_p,
                fallback_used=cold,
                inference_time_ms=infer_ms,
            ))
        return preds

    # -- warm-start state (survives restarts; lets a standby take over) ------
    def snapshot(self) -> None:
        """Atomically persist the rolling buffer so a restarted/standby instance
        warm-starts instead of rebuilding history from scratch."""
        path = self.cfg.state_path
        if not path:
            return
        with self._lock:
            buffer, current, seen = self._buffer.copy(), self._current.copy(), self._seen.copy()
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            tmp = path + ".tmp.npz"
            with open(tmp, "wb") as fh:
                np.savez(fh, buffer=buffer, current=current, seen=seen,
                         meta=np.array([self.n, self.topo.graph_version], dtype=object))
            os.replace(tmp, path)
        except Exception as exc:  # noqa: BLE001 — snapshotting is best-effort
            log.debug("state snapshot failed: %s", exc)

    def _restore(self) -> None:
        path = self.cfg.state_path
        if not path or not os.path.exists(path):
            return
        try:
            with np.load(path, allow_pickle=True) as z:
                meta = z["meta"]
                if int(meta[0]) != self.n or str(meta[1]) != self.topo.graph_version:
                    log.warning("ignoring stale state snapshot (n/graph_version mismatch)")
                    return
                self._buffer = z["buffer"].astype(np.float32)
                self._current = z["current"].astype(np.float32)
                self._seen = z["seen"].astype(np.int32)
            log.info("warm-started rolling buffer from %s (%d delayed stations)",
                     path, int(np.count_nonzero(self._current)))
        except Exception as exc:  # noqa: BLE001
            log.warning("state restore failed (%s); starting cold", exc)

    def forward_latency_ms(self, runs: int = 1) -> float:
        """Single synchronous forward used by the /predict probe and load test."""
        with self._lock:
            window = self._buffer.copy()
        x = torch.from_numpy(window).view(1, 1, self.cfg.history_steps, self.n)
        t0 = time.perf_counter()
        with torch.no_grad():
            for _ in range(max(1, runs)):
                self.model(x, self.l_tilde)
        return (time.perf_counter() - t0) * 1000.0 / max(1, runs)
