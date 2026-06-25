"""STGCN forward-pass shape + numerical-sanity tests (no Kafka, no graph engine)."""
from __future__ import annotations

import numpy as np
import torch

from src.config import Settings
from src.model.inference import InferenceEngine
from src.model.stgcn_network import STGCN


def test_forward_returns_per_node_scalar(ring_topology):
    model = STGCN(n_nodes=ring_topology.n, hidden=16, order=3, kt=3, history=12).eval()
    x = torch.randn(1, 1, 12, ring_topology.n)
    with torch.no_grad():
        y = model(x, ring_topology.l_tilde_torch)
    assert y.shape == (1, ring_topology.n)
    assert torch.isfinite(y).all()


def test_escalation_ratio_is_bounded(ring_topology):
    cfg = Settings(history_steps=12, cheb_order=3, hidden_channels=16, min_warm_steps=3)
    engine = InferenceEngine(ring_topology, cfg)
    # inject a sizeable delay at one station across several frames
    for _ in range(5):
        engine.observe(3, 480.0, "RER-A:T1")
        preds = engine.step_and_infer()
    by_station = {p.station_id: p for p in preds}
    assert 3 in by_station, "a delayed station should produce a prediction"
    p = by_station[3]
    assert 0.0 <= p.escalation_ratio <= 1.0
    assert p.median_s >= 0.0
    assert p.p95_s >= p.p90_s >= 0.0


def test_cold_station_uses_fallback(ring_topology):
    cfg = Settings(history_steps=12, min_warm_steps=4)
    engine = InferenceEngine(ring_topology, cfg)
    engine.observe(10, 600.0, "RER-B:T9")
    preds = engine.step_and_infer()           # only one observation -> cold
    p = next(x for x in preds if x.station_id == 10)
    assert p.fallback_used is True


def test_quiet_network_emits_nothing(ring_topology):
    cfg = Settings(history_steps=12)
    engine = InferenceEngine(ring_topology, cfg)
    engine.observe(1, 5.0, "x")               # below the relevance floor
    assert engine.step_and_infer() == []
