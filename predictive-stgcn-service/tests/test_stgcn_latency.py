"""DoD latency check: single-window STGCN inference P99 <= 35 ms.

Runs the real forward path many times over a realistically-sized network and
asserts the 99th percentile stays within budget. Marked so it can be skipped on
constrained CI runners, but it runs by default.
"""
from __future__ import annotations

import time

import numpy as np
import pytest
import scipy.sparse as sp
import torch

from src.config import Settings
from src.graph.topology_client import Topology, _to_torch_sparse
from src.model.inference import InferenceEngine

N_NODES = 392          # the real IDFM rail network size
ITERATIONS = 300
P99_BUDGET_MS = 35.0


def _grid_topology(n: int) -> Topology:
    rng = np.random.default_rng(11)
    rows, cols = [], []
    for i in range(n):
        for j in (i + 1, i + 3):           # nearest + skip neighbour, wrapped
            k = j % n
            rows += [i, k]
            cols += [k, i]
    adj = sp.coo_matrix((np.ones(len(rows)), (rows, cols)), shape=(n, n)).tocsr()
    deg = np.asarray(adj.sum(axis=1)).ravel()
    d = sp.diags(np.divide(1.0, np.sqrt(deg), out=np.zeros_like(deg), where=deg > 0))
    lap = sp.eye(n) - d @ adj @ d
    l_tilde = (lap - sp.eye(n)).tocoo()    # λmax≈2 -> (2/2)L - I = L - I
    return Topology(
        n=n, graph_version="bench", lambda_max=2.0,
        l_tilde_coo=l_tilde, l_tilde_csr=l_tilde.tocsr(),
        l_tilde_torch=_to_torch_sparse(l_tilde),
        station_names=[f"S{i}" for i in range(n)],
        pagerank=np.full(n, 1.0 / n),
    )


def test_inference_p99_under_budget():
    torch.set_num_threads(1)
    cfg = Settings(history_steps=12, cheb_order=3, hidden_channels=32, torch_threads=1)
    engine = InferenceEngine(_grid_topology(N_NODES), cfg)

    for _ in range(20):                    # warm up JIT/alloc paths
        engine.forward_latency_ms(1)

    samples = []
    for _ in range(ITERATIONS):
        t0 = time.perf_counter()
        engine.forward_latency_ms(1)
        samples.append((time.perf_counter() - t0) * 1000.0)

    samples.sort()
    p99 = samples[int(0.99 * (len(samples) - 1))]
    print(f"\nSTGCN N={N_NODES} p50={samples[len(samples)//2]:.2f}ms "
          f"p99={p99:.2f}ms max={samples[-1]:.2f}ms")
    assert p99 <= P99_BUDGET_MS, f"P99 {p99:.2f}ms exceeds {P99_BUDGET_MS}ms budget"
